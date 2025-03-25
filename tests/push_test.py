#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import os
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
from tests.setup import setup,  base_config
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

def test_push(tmp_path):
    """Run and validate the importer"""
    setup(tmp_path)
    importer = importers.DirectoryImporter("https://prefix")
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
    }
    git = Git(base_config.git_dir)
    commit_metadata = CommitMetadata()
    git_annex_settings = GitAnnexSettings(commit_metadata, b'git-annex')
    with (
        LocalRepo(bytes(base_config.git_dir, encoding='utf8')) as repo,
        DoltSqlServer(base_config.dolt_dir, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with AnnexCache(repo, dolt_server, git, git_annex_settings, base_config.auto_push, import_config.batch_size) as cache:
            downloader = GitAnnexDownloader(
                cache = cache,
                git = git,
                dolt_server = dolt_server,
            )
            do_import(import_config, downloader, importer, ["import_data"])
            downloader.flush()
            files_pushed = do_push(downloader, "origin", [])
            assert files_pushed == 2
            downloader.flush()
            files_pushed = do_push(downloader, "origin", [])
            assert files_pushed == 0
        
