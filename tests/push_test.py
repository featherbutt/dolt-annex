#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import os
import random
import shutil

from annex import AnnexCache, GitAnnexSettings
from bup.repo import LocalRepo
from bup_ext.bup_ext import CommitMetadata
from commands.import_command import ImportConfig, do_import
from commands.push import do_push
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import importers
import move_functions
from tests.setup import setup_file_remote, setup_ssh_remote, base_config
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
            do_import(import_config, downloader, importer, ["import_data"])
            downloader.flush()
            files_pushed = do_push(downloader, "origin", [], ssh_config, known_hosts)
            assert files_pushed == 2
            downloader.flush()
            files_pushed = do_push(downloader, "origin", [], ssh_config, known_hosts)
            assert files_pushed == 0
        
