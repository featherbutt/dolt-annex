#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import os
from pathlib import Path
import random
import shutil
from typing_extensions import Optional
import uuid

import paramiko 

from dolt_annex.table import FileTable
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.pull import do_pull
from dolt_annex.commands.server_command import server_context
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.filestore import get_key_path
from dolt_annex import move_functions, importers
from dolt_annex.datatypes import Remote, FileTableSchema, TableRow
from dolt_annex.commands.sync import SshSettings

from tests.setup import setup, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = Path(__file__).parent / "import_data"
config_directory = Path(__file__).parent / "config"
remote_annex = Path(__file__).parent / "test_annex"

def test_pull_local(tmp_path):
    origin_uuid = uuid.uuid4()
    setup(tmp_path, origin_uuid)
    init()
    remote = Remote(
        url=f"file://{tmp_path}/remote_files",
        uuid=origin_uuid,
        name="origin",
    )
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
    remote = Remote(
        url=f"file://{tmp_path}/remote_files",
        uuid=origin_uuid,
        name="origin",
    )
    
    # setup server, then create server context, then setup client.
    with server_context(host, ssh_port, server_key, str(Path(__file__).parent / "test_client_keys")):
        init()
        do_test_pull(tmp_path, "submissions", remote)

class TestImporter(importers.ImporterBase):
    def key_columns(self, path: Path) -> Optional[TableRow]:
        sid = int(''.join(path.parts[-6:-1]))
        return TableRow(("furaffinity.net", sid, '2021-01-01', 1))

def do_test_pull(tmp_path, table_name: str, remote: Remote):
    """Run and validate pulling content files from a remote"""
    importer = TestImporter()
    with contextlib.chdir(config_directory):
        table = FileTableSchema.from_name(table_name)
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    ssh_settings = SshSettings(Path(__file__).parent / "config/ssh_config", None)
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
        FileTable(dolt_server, table, base_config.auto_push, import_config.batch_size) as downloader
    ):
        do_import(remote, import_config, downloader, importer, ["import_data/00"])
        downloader.flush()
        with downloader.dolt.set_branch(f"{remote.uuid}-{table.name}"):
            dolt_server.commit(amend=True)
        files_pulled = pull_and_verify(downloader, base_config.files_dir, remote, ssh_settings, table)
        assert files_pulled == 2
        # Pulling again should have no effect

        files_pulled = pull_and_verify(downloader, base_config.files_dir, remote, ssh_settings, table)
        assert files_pulled == 0


def pull_and_verify(downloader: FileTable, files_dir: Path, file_remote: Remote, ssh_settings: SshSettings, file_key_table: FileTableSchema):

    files_pulled = do_pull(downloader, file_remote, ssh_settings, file_key_table, [], limit=None)
    downloader.flush()
        
    for key in files_pulled:
        key_path = files_dir / get_key_path(key)
        assert Path(key_path).exists()
    # TODO: Test that the branches are correct.

    return len(files_pulled)
