#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import random
import shutil
from typing_extensions import Optional, List
import uuid

import paramiko 

from annex import AnnexCache, SubmissionId
from commands.import_command import ImportConfig, do_import
from commands.push import do_push
from commands.server_command import server_context
import context
from dolt import DoltSqlServer
from git import get_key_path
import importers
import move_functions
from remote import Remote
from commands.sync import SshSettings

from tables import FileKeyTable
from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init
from type_hints import TableRow

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")
config_directory = Path(__file__).parent / "config"

batch_size = 10

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
    remote = Remote(
        url=f"file://{tmp_path}/remote_files",
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

def do_test_push(tmp_path, table_name: str, remote: Remote):
    """Run and validate pushing content files to a remote"""
    importer = TestImporter()
    shutil.copy(config_directory / "submissions.table", tmp_path / "submissions.table")
    shutil.copy(config_directory / "urls.table", tmp_path / "urls.table")
    table = FileKeyTable.from_name(table_name)
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with AnnexCache(dolt_server, table, base_config.auto_push, import_config.batch_size) as downloader:
            ssh_settings = SshSettings(Path(__file__).parent / "config/ssh_config", None)
            file_key_table = FileKeyTable.from_name("submissions")
            do_import(import_config, downloader, importer, ["import_data/00"])
            downloader.flush()
            with downloader.dolt.set_branch(f"{context.local_uuid.get()}-{table.name}"):
                dolt_server.commit(amend=True)

            files_pushed = push_and_verify(downloader, remote, ssh_settings, file_key_table)
            assert files_pushed == 2
            # Pushing again should have no effect

            files_pushed = push_and_verify(downloader, remote, ssh_settings, file_key_table)
            assert files_pushed == 0

            # But if we add more files, it should push them
            do_import(import_config, downloader, importer, ["import_data/08"])
            downloader.flush()
            with downloader.dolt.set_branch(f"{context.local_uuid.get()}-{table.name}"):
                dolt_server.commit(amend=True)

            files_pushed = push_and_verify(downloader, remote, ssh_settings, file_key_table)
            assert files_pushed == 1


def push_and_verify(downloader: AnnexCache, file_remote: Remote, ssh_settings: SshSettings, file_key_table: FileKeyTable):

    files_pushed = do_push(downloader, file_remote, ssh_settings, file_key_table, [], limit=None)
    downloader.flush()

    if file_remote.url.startswith("file://"):
        pushed_files_dir = Path(file_remote.url[7:]).absolute()
    elif '@' in file_remote.url:
        user, rest = file_remote.url.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        pushed_files_dir = Path(path).absolute()
    else:
        raise ValueError(f"Unsupported remote URL format: {file_remote.url}")
        
    for key in files_pushed:
        
        key_path = pushed_files_dir / get_key_path(key)
        assert Path(key_path).exists()
    # TODO: Test that the branches are correct.

    return len(files_pushed)
