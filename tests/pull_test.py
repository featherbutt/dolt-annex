#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import os
from pathlib import Path
import random
import shutil
from typing import override
import uuid
from typing_extensions import Optional

import paramiko 

from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.filestore import FileStore
from dolt_annex.table import Dataset
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.pull import pull_dataset
from dolt_annex.commands.server_command import server_context
from dolt_annex import move_functions, importers
from dolt_annex.datatypes import TableRow
from dolt_annex.datatypes.remote import Repo
from dolt_annex.file_keys.sha256e import Sha256e
from tests.import_test import TestImporter

from .setup import setup, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
    file_key_type = Sha256e
)

import_directory = Path(__file__).parent / "import_data"
config_directory = Path(__file__).parent / "config"
remote_annex = Path(__file__).parent / "test_annex"

def test_pull_local(tmp_path):
    origin_uuid = uuid.uuid4()
    setup(tmp_path, origin_uuid)
    init()
    remote = Repo.model_validate({
        "files_url": f"file://{tmp_path}/remote_files",
        "uuid": origin_uuid,
        "name": "origin",
        "key_format": "Sha256e"
    })
    do_test_pull(tmp_path, "submissions", remote)

def test_pull_sftp(tmp_path):
    with setup_ssh_remote(tmp_path) as remote:
        do_test_pull(tmp_path, "submissions", remote)

def test_pull_server(tmp_path):
    print(tmp_path)
    os.chdir(tmp_path)
    origin_uuid = uuid.uuid4()
    
    server_key = paramiko.RSAKey.generate(bits=1024)
    host = "localhost"
    ssh_port = random.randint(21000, 22000)
    setup(tmp_path, origin_uuid)
    remote = Repo.model_validate({
        "files_url": f"file://{tmp_path}/remote_files",
        "uuid": origin_uuid,
        "name": "origin",
    })

    # setup server, then create server context, then setup client.
    with server_context(host, ssh_port, server_key, str(Path(__file__).parent / "test_client_keys")):
        init()
        do_test_pull(tmp_path, "submissions", remote)

def do_test_pull(tmp_path, dataset_name: str, remote: Repo):
    """Run and validate pulling content files from a remote"""
    tmp_path = Path(tmp_path)
    importer = TestImporter()
    remote_file_store = remote.filestore()
    local_file_store = base_config.get_filestore()
    with contextlib.chdir(config_directory):
        dataset_schema = DatasetSchema.must_load(dataset_name)
    shutil.copytree(import_directory, tmp_path / "import_data")
    with (
        local_file_store.open(base_config),
        remote_file_store.open(base_config),
        Dataset.connect(base_config, import_config.batch_size, dataset_schema) as dataset
    ):
        do_import(remote_file_store, remote.uuid, import_config, dataset, importer, ["import_data/00"])
        dataset.flush()
        assert remote_file_store.exists(Sha256e.from_file(import_directory / "00/12/34/56/78/591785b794601e212b260e25925636fd.e621.txt"))

        with dataset.dolt.set_branch(f"{remote.uuid}-{dataset.name}"):
            dataset.dolt.commit(amend=True)
        files_pulled = pull_and_verify(dataset, remote, remote_file_store, local_file_store)
        assert files_pulled == 2
        # Pulling again should have no effect

        files_pulled = pull_and_verify(dataset, remote, remote_file_store, local_file_store)
        assert files_pulled == 0


def pull_and_verify(dataset: Dataset, file_remote: Repo, remote_file_store: FileStore, local_file_store: FileStore) -> int:

    files_pulled = pull_dataset(dataset, base_config.get_uuid(), file_remote, remote_file_store, local_file_store, [])
    dataset.flush()
        
    for key in files_pulled:
        assert local_file_store.exists(key)
    # TODO: Test that the branches are correct.

    return len(files_pulled)
