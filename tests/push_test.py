#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import random
import shutil
from typing import Optional, List
import uuid

import paramiko 

from annex import AnnexCache, SubmissionId
from commands.import_command import ImportConfig, do_import
from commands.push import do_push
from commands.server_command import server_context
from config import config
import context
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
import importers
import move_functions
from remote import Remote
from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init
from type_hints import UUID

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")

batch_size = 10

def test_push_local(tmp_path):
    remote = setup_file_remote(tmp_path)
    do_test_push(tmp_path, remote)

def test_push_sftp(tmp_path):
    with setup_ssh_remote(tmp_path) as remote:
        do_test_push(tmp_path, remote)

def test_push_server(tmp_path):
    print(tmp_path)
    os.chdir(tmp_path)
    origin_uuid = uuid.uuid4()
    
    server_key = paramiko.RSAKey.generate(bits=1024)
    host = "localhost"
    ssh_port = random.randint(21000, 22000)
    setup(tmp_path, origin_uuid)
    remote = Remote(
        url=f"file://{tmp_path}/files",
        uuid=origin_uuid,
        name="origin",
    )
    
    # setup server, then create server context, then setup client.
    with server_context(host, ssh_port, server_key, str(Path(__file__).parent / "test_client_keys")):
        init()
        do_test_push(tmp_path, remote)

class TestImporter(importers.Importer):
    def __init__(self, prefix_url: str):
        self.prefix_url = prefix_url

    def url(self, abs_path: str, rel_path: str) -> List[str]:
        return [f"{self.prefix_url}/{rel_path}"]
    
    def md5(self, path: str) -> str | None:
        return None
    
    def submission_id(self, abs_path: str, rel_path: str) -> Optional[SubmissionId]:
        parts = abs_path.split(os.path.sep)
        sid = int(''.join(parts[-6:-1]))
        return SubmissionId("furaffinity.net", sid, '2021-01-01', 1)

    def skip(self, path: str) -> bool:
        return False

def do_test_push(tmp_path, remote: Remote):
    """Run and validate pushing content files to a remote"""
    importer = TestImporter("https://prefix")
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    with (
        DoltSqlServer(base_config.dolt_dir, db_config, base_config.spawn_dolt_server, base_config.gc) as dolt_server,
    ):
        with AnnexCache(dolt_server, base_config.auto_push, import_config.batch_size) as cache:
            downloader = GitAnnexDownloader(
                cache = cache,
                dolt_server = dolt_server,
            )
            ssh_config = os.path.join(os.path.dirname(__file__), "config/ssh_config")
            known_hosts = None
            do_import(import_config, downloader, importer, ["import_data/00"])
            downloader.flush()
            with downloader.dolt_server.set_branch("files"):
                dolt_server.commit(amend=True)
            with downloader.dolt_server.set_branch(context.local_uuid.get()):
                dolt_server.commit(amend=True)

            files_pushed = push_and_verify(downloader, remote, dolt_server, ssh_config, known_hosts)
            assert files_pushed == 2
            # Pushing again should have no effect

            files_pushed = push_and_verify(downloader, remote, dolt_server, ssh_config, known_hosts)
            assert files_pushed == 0

            # But if we add more files, it should push them
            do_import(import_config, downloader, importer, ["import_data/08"])
            downloader.flush()
            with downloader.dolt_server.set_branch("files"):
                dolt_server.commit(amend=True)
            with downloader.dolt_server.set_branch(context.local_uuid.get()):
                dolt_server.commit(amend=True)

            files_pushed = push_and_verify(downloader, remote, dolt_server, ssh_config, known_hosts)
            assert files_pushed == 1


def push_and_verify(downloader: GitAnnexDownloader, file_remote: Remote, dolt_server: DoltSqlServer, ssh_config: str, known_hosts: Optional[str]):

    files_pushed = do_push(downloader, file_remote, [], ssh_config, known_hosts, None)
    downloader.flush()

    # Check that the dolt branches were pushed.
    remote_uuid = file_remote.uuid
    # TODO: Test that files are actually moved to the remote and that the branches are correct.
    assert dolt_server.get_revision("origin/files") == dolt_server.get_revision("files")
    assert dolt_server.get_revision(f"origin/{context.local_uuid.get()}") == dolt_server.get_revision(context.local_uuid.get())
    assert dolt_server.get_revision(f"origin/{remote_uuid}") == dolt_server.get_revision(remote_uuid)

    return files_pushed
