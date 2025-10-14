#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import random
import shutil
from typing_extensions import Optional
import uuid

import paramiko 

from dolt_annex import importers, move_functions, context 
from dolt_annex.datatypes.table import DatasetSchema, DatasetSource
from dolt_annex.table import Dataset
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.push import push_dataset
from dolt_annex.commands.server_command import server_context
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.filestore import get_key_path
from dolt_annex.datatypes import Repo, TableRow
from dolt_annex.commands.sync import SshSettings

from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")
config_directory = Path(__file__).parent / "config"

def test_push_local(tmp_path):
    remote = setup_file_remote(tmp_path)
    do_test_push(tmp_path, "submissions", remote)

def test_push_sftp(tmp_path):
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
        files_url=f"file://{tmp_path}/remote_files",
        uuid=origin_uuid,
        name="origin",
    )
    
    # setup server, then create server context, then setup client.
    with server_context(host, ssh_port, server_key, str(Path(__file__).parent / "test_client_keys")):
        init()
        do_test_push(tmp_path, "submissions", remote)

class TestImporter(importers.ImporterBase):
    def key_columns(self, path: Path) -> Optional[TableRow]:
        sid = int(''.join(path.parts[-6:-1]))
        return TableRow(("furaffinity.net", sid, '2021-01-01', 1))

def do_test_push(tmp_path, dataset_name: str, remote: Repo):
    """Run and validate pushing content files to a remote"""
    importer = TestImporter()
    shutil.copy(config_directory / "submissions.dataset", tmp_path / "submissions.dataset")
    shutil.copy(config_directory / "urls.dataset", tmp_path / "urls.dataset")
    dataset_schema = DatasetSchema.must_load(dataset_name)
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    dataset_source = DatasetSource(dataset_schema, repo=base_config.local_repo())
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with Dataset(dolt_server, dataset_source, base_config.auto_push, import_config.batch_size) as downloader:
            ssh_settings = SshSettings(Path(__file__).parent / "config/ssh_config", None)
            local_remote = base_config.local_repo()
            table = next(iter(downloader.tables.values()))
            do_import(local_remote, import_config, table, importer, ["import_data/00"])
            downloader.flush()
            with downloader.dolt.set_branch(f"{context.local_uuid.get()}-{dataset_name}"):
                dolt_server.commit(amend=True)

            files_pushed = push_and_verify(downloader, remote, ssh_settings)
            assert files_pushed == 2
            # Pushing again should have no effect

            files_pushed = push_and_verify(downloader, remote, ssh_settings)
            assert files_pushed == 0

            # But if we add more files, it should push them
            do_import(local_remote, import_config, table, importer, ["import_data/08"])
            downloader.flush()
            with downloader.dolt.set_branch(f"{context.local_uuid.get()}-{dataset_name}"):
                dolt_server.commit(amend=True)

            files_pushed = push_and_verify(downloader, remote, ssh_settings)
            assert files_pushed == 1


def push_and_verify(downloader: Dataset, file_remote: Repo, ssh_settings: SshSettings):

    files_pushed = push_dataset(downloader, file_remote, ssh_settings, [], limit=None)
    downloader.flush()

    if file_remote.files_url.startswith("file://"):
        pushed_files_dir = Path(file_remote.files_url[7:]).absolute()
    elif '@' in file_remote.files_url:
        user, rest = file_remote.files_url.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        pushed_files_dir = Path(path).absolute()
    else:
        raise ValueError(f"Unsupported remote URL format: {file_remote.files_url}")
        
    for key in files_pushed:
        
        key_path = pushed_files_dir / get_key_path(key)
        assert Path(key_path).exists()
    # TODO: Test that the branches are correct.

    return len(files_pushed)
