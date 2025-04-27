#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import os
from pathlib import Path
import random
import shutil

import paramiko 

from annex import AnnexCache, GitAnnexSettings
from bup.repo import LocalRepo
from bup_ext.bup_ext import CommitMetadata
from commands.import_command import ImportConfig, do_import
from commands.push import do_push
from commands.server_command import server_context
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import importers
import move_functions
from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init
from type_hints import AnnexKey

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")

def key_from_bytes(data: bytes, extension: str) -> AnnexKey:
    data_hash = hashlib.sha256(data).hexdigest()
    return AnnexKey(f"SHA256E-s{len(data)}--{data_hash}.{extension}")

batch_size = 10

def test_push_local(tmp_path):
    setup_file_remote(tmp_path)
    do_test_push(tmp_path)

def test_push_sftp(tmp_path):
    with setup_ssh_remote(tmp_path):
        do_test_push(tmp_path)

def test_push_server(tmp_path):
    print(tmp_path)
    os.chdir(tmp_path)
    #Path("./client").mkdir()
    #Path("./server").mkdir()
    
    server_key = paramiko.RSAKey.generate(bits=1024)
    host = "localhost"
    ssh_port = random.randint(21000, 22000)
    setup(tmp_path)

    server_git = Git(f"{tmp_path}/git_origin")
    
    # setup server, then create server context, then setup client.
    with server_context(server_git, host, ssh_port, server_key, str(Path(__file__).parent / "test_client_keys")):
        init(f"ssh://git@localhost:{ssh_port}/{tmp_path}/git_origin")
        do_test_push(tmp_path)

def do_test_push(tmp_path):
    """Run and validate pushing content files to a remote"""
    importer = importers.DirectoryImporter("https://prefix")
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    git = Git(base_config.git_dir)
    commit_metadata = CommitMetadata()
    git_annex_settings = GitAnnexSettings(commit_metadata, b'git-annex')
    with (
        LocalRepo(bytes(base_config.git_dir, encoding='utf8')) as repo,
        DoltSqlServer(base_config.dolt_dir, db_config, base_config.spawn_dolt_server, base_config.gc) as dolt_server,
    ):
        with AnnexCache(repo, dolt_server, git, git_annex_settings, base_config.auto_push, import_config.batch_size) as cache:
            downloader = GitAnnexDownloader(
                cache = cache,
                git = git,
                dolt_server = dolt_server,
            )
            ssh_config = os.path.join(os.path.dirname(__file__), "config/ssh_config")
            known_hosts = None
            do_import(import_config, downloader, importer, ["import_data/00"])
            downloader.flush()

            files_pushed = push_and_verify(downloader, git, dolt_server, ssh_config, known_hosts)
            assert files_pushed == 2
            # Pushing again should have no effect

            files_pushed = push_and_verify(downloader, git, dolt_server, ssh_config, known_hosts)
            assert files_pushed == 0

            # But if we add more files, it should push them
            do_import(import_config, downloader, importer, ["import_data/08"])
            downloader.flush()

            files_pushed = push_and_verify(downloader, git, dolt_server, ssh_config, known_hosts)
            assert files_pushed == 1


def push_and_verify(downloader: GitAnnexDownloader, git: Git, dolt_server: DoltSqlServer, ssh_config: str, known_hosts: str):
    files_pushed = do_push(downloader, "origin", "origin", [], ssh_config, known_hosts)
    downloader.flush()
    # Check that git branch was pushed.
    assert git.get_revision("origin/git-annex") == git.get_revision("git-annex")

    # Check that the dolt branches were pushed.
    remote_uuid = git.annex.get_remote_uuid("origin")
    assert dolt_server.get_revision("origin/main") == dolt_server.get_revision("main")
    assert dolt_server.get_revision(f"origin/{git.annex.uuid}") == dolt_server.get_revision(git.annex.uuid)
    assert dolt_server.get_revision(f"origin/{remote_uuid}") == dolt_server.get_revision(remote_uuid)

    return files_pushed
