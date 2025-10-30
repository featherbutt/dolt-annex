#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import os
from pathlib import Path
import random
import shutil
import uuid

import paramiko 

from dolt_annex import move_functions 
from dolt_annex.datatypes.remote import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore import FileStore
from dolt_annex.table import Dataset
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.push import push_dataset
from dolt_annex.commands.server_command import server_context

from tests.import_test import TestImporter
from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
    file_key_type=Sha256e
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")
config_directory = Path(__file__).parent / "config"

def test_push_local(tmp_path):
    remote = setup_file_remote(tmp_path)
    do_test_push(tmp_path, "submissions", remote)

def test_push_sftp(tmp_path):
    base_config.ssh.encrypted_ssh_key = False

    with setup_ssh_remote(tmp_path) as remote:
        do_test_push(tmp_path, "submissions", remote)

def test_push_server(tmp_path):
    print(tmp_path)
    os.chdir(tmp_path)
    origin_uuid = uuid.uuid4()
    
    server_key = paramiko.RSAKey.generate(bits=1024)
    host = "localhost"
    ssh_port = random.randint(21000, 22000)
    setup(tmp_path, origin_uuid)
    remote = Repo(
        files_url=f"ssh://localhost:{ssh_port}/{tmp_path}/remote_files",
        uuid=origin_uuid,
        name="origin",
        key_format=Sha256e
    )
    
    # setup server, then create server context, then setup client.
    with server_context(host, ssh_port, server_key, str(Path(__file__).parent / "test_client_keys")):
        init()
        do_test_push(tmp_path, "submissions", remote)

def do_test_push(tmp_path, dataset_name: str, remote: Repo):
    """Run and validate pushing content files to a remote"""
    tmp_path = Path(tmp_path)
    importer = TestImporter()
    shutil.copy(config_directory / "submissions.dataset", tmp_path / "submissions.dataset")
    shutil.copy(config_directory / "urls.dataset", tmp_path / "urls.dataset")
    shutil.copytree(import_directory, tmp_path / "import_data")

    importer = TestImporter()
    remote_file_store = remote.filestore()
    local_file_store = base_config.get_filestore()
    local_uuid = base_config.get_uuid()
    with contextlib.chdir(config_directory):
        dataset_schema = DatasetSchema.must_load(dataset_name)
    
    with (
        local_file_store.open(base_config),
        remote_file_store.open(base_config),
        Dataset.connect(base_config, import_config.batch_size, dataset_schema) as dataset
    ):
            do_import(local_file_store, local_uuid, import_config, dataset, importer, ["import_data/00"])
            dataset.flush()
            with dataset.dolt.set_branch(f"{local_uuid}-{dataset_name}"):
                dataset.dolt.commit(amend=True)

            files_pushed = push_and_verify(dataset, remote, local_uuid, remote_file_store, local_file_store)
            assert files_pushed == 2
            # Pushing again should have no effect

            files_pushed = push_and_verify(dataset, remote, local_uuid, remote_file_store, local_file_store)
            assert files_pushed == 0

            # But if we add more files, it should push them
            do_import(local_file_store, local_uuid, import_config, dataset, importer, ["import_data/08"])
            dataset.flush()
            with dataset.dolt.set_branch(f"{local_uuid}-{dataset_name}"):
                dataset.dolt.commit(amend=True)

            files_pushed = push_and_verify(dataset, remote, local_uuid, remote_file_store, local_file_store)
            assert files_pushed == 1


def push_and_verify(dataset: Dataset, file_remote: Repo, local_uuid: uuid.UUID, remote_file_store: FileStore, local_file_store: FileStore):

    files_pushed = push_dataset(dataset, local_uuid, file_remote, remote_file_store, local_file_store, [], limit=None)
    dataset.flush()

    for key in files_pushed:
        assert remote_file_store.exists(key), f"Remote filestore missing pushed file {key}"
    # TODO: Test that the branches are correct.

    return len(files_pushed)
