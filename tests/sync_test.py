#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import random
import shutil
import uuid
import copy
from typing_extensions import Optional, List

import paramiko 

from dolt_annex import importers, move_functions, config, context
from dolt_annex.table import FileTable
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.server_command import server_context
from dolt_annex.commands.sync import SshSettings, SyncResults, TableFilter, do_sync
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.filestore import get_key_path
from dolt_annex.datatypes import Repo, FileTableSchema, TableRow
from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")

batch_size = 10

def test_sync_local(tmp_path):
    remote = setup_file_remote(tmp_path)
    do_test_sync(tmp_path, remote)

def test_sync_sftp(tmp_path):
    with setup_ssh_remote(tmp_path) as remote:
        do_test_sync(tmp_path, remote)

def test_sync_server(tmp_path):
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
        do_test_sync(tmp_path, remote)

class TestImporter(importers.ImporterBase):
    
    def key_columns(self, path: Path) -> Optional[TableRow]:
        sid = int(''.join(path.parts[-6:-1]))
        return TableRow(("furaffinity.net", sid, '2021-01-01', 1))

def do_test_sync(tmp_path, remote: Repo):
    """Run and validate syncing content files to a remote"""
    importer = TestImporter()
    table = FileTableSchema.from_name("submissions")
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
        FileTable(dolt_server, table, base_config.auto_push, import_config.batch_size) as cache
    ):
            first_downloader = cache

            other_base_config = copy.replace(base_config, files_dir = "./other_files")

            with (
                config.config_context(other_base_config),
                context.assign(context.local_uuid, uuid.uuid4())
            ):
                second_downloader = cache
                
            ssh_settings = SshSettings(
                ssh_config = Path(__file__).parent / "config" / "ssh_config",
                known_hosts = None
            )
            do_import(local_remote, import_config, first_downloader, importer, ["import_data/00"])
            first_downloader.flush()
            with first_downloader.dolt.set_branch("files"):
                dolt_server.commit(amend=True)
            with first_downloader.dolt.set_branch(context.local_uuid.get()):
                dolt_server.commit(amend=True)

            file_key_table = FileTableSchema.from_name("submissions")

            # Sync first local repo with the remote, only pushing files
            filters: List[TableFilter] = []
            files_synced = sync_and_verify(first_downloader, remote, ssh_settings, file_key_table, filters)
            assert len(files_synced.files_pushed) == 2
            assert len(files_synced.files_pulled) == 0
            # Pushing again should have no effect

            files_synced = sync_and_verify(first_downloader, remote, ssh_settings, file_key_table, filters)
            assert len(files_synced.files_pushed) == 0
            assert len(files_synced.files_pulled) == 0

            # Sync second local repo with the remote, pushing and pulling files

            do_import(local_remote, import_config, second_downloader, importer, ["import_data/08"])
            second_downloader.flush()
            with second_downloader.dolt.set_branch("files"):
                dolt_server.commit(amend=True)
            with second_downloader.dolt.set_branch(context.local_uuid.get()):
                dolt_server.commit(amend=True)

            files_synced = sync_and_verify(second_downloader, remote, ssh_settings, file_key_table, filters)
            assert len(files_synced.files_pushed) == 1
            assert len(files_synced.files_pulled) == 2

            files_synced = sync_and_verify(second_downloader, remote, ssh_settings, file_key_table, filters)
            assert len(files_synced.files_pushed) == 0
            assert len(files_synced.files_pulled) == 0
            
            # Sync first local repo again, only pulling files
            files_synced = sync_and_verify(first_downloader, remote, ssh_settings, file_key_table, filters)
            assert len(files_synced.files_pushed) == 0
            assert len(files_synced.files_pulled) == 1
            # Pushing again should have no effect

            files_synced = sync_and_verify(first_downloader, remote, ssh_settings, file_key_table, filters)
            assert len(files_synced.files_pushed) == 0
            assert len(files_synced.files_pulled) == 0
            


def sync_and_verify(downloader: FileTable, file_remote: Repo, ssh_settings: SshSettings, file_key_table: FileTableSchema, filters: List[TableFilter]) -> SyncResults:

    files_synced = do_sync(downloader, file_remote, ssh_settings, file_key_table, filters)
    downloader.flush()

    if file_remote.files_url.startswith("file://"):
        pushed_files_dir = Path(file_remote.files_url[7:]).absolute()
    elif '@' in file_remote.files_url:
        user, rest = file_remote.files_url.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        pushed_files_dir = Path(path).absolute()
    else:
        raise ValueError(f"Unsupported remote URL format: {file_remote.files_url}")
        
    for key in files_synced.files_pushed:
        key_path = pushed_files_dir / get_key_path(key)
        assert Path(key_path).exists()

    for key in files_synced.files_pulled:
        key_path = Path("./dolt") / get_key_path(key)
        assert Path(key_path).exists()

    return files_synced
