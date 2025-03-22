#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import os
import shutil
import pytest

from plumbum import local # type: ignore

from annex import AnnexCache, GitAnnexSettings
from application import Downloader
from bup.repo import LocalRepo
from bup_ext.bup_ext import CommitMetadata
from commands.import_command import ImportConfig, do_import
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import importers
from tests.setup import setup,  base_config
from type_hints import AnnexKey
from db import get_annex_key_from_url, get_sources_from_annex_key, get_urls_from_annex_key, is_key_present

def key_from_bytes(data: bytes, extension: str) -> AnnexKey:
    data_hash = hashlib.sha256(data).hexdigest()
    return AnnexKey(f"SHA256E-s{len(data)}--{data_hash}.{extension}")

expected_urls = {
    "591785b794601e212b260e25925636fd.txt": "https://prefix/import_data/nested/591785b794601e212b260e25925636fd.txt",
    "b1946ac92492d2347c6235b4d2611184.txt": "https://prefix/import_data/b1946ac92492d2347c6235b4d2611184.txt",
}

def test_import_with_prefix_url(tmp_path):
    """Test importing with a url determined by the file path"""
    setup(tmp_path)
    import_directory = os.path.join(os.path.dirname(__file__), "import_data")
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    import_config = ImportConfig(
        batch_size = 10,
        move_function = shutil.move,
        follow_symlinks = False,
    )
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
            importer = importers.DirectoryImporter("https://prefix")
            do_import(import_config, downloader, importer, ["import_data"])
        # Check that the files were imported

        assert os.path.exists("git/annex")
        file_count = 0
        for root, _, files in os.walk(import_directory):
            for file in files:
                file_count += 1
                extension = file.split(".")[-1]
                with open(os.path.join(root, file), "rb") as f:
                    key = key_from_bytes(f.read(), extension)
                assert_key(git, dolt_server, key, expected_urls[file])

    assert file_count == 2

def assert_key(git: Git, dolt: DoltSqlServer, key: AnnexKey, expected_url: str):
    # 1. Check the annexed file exists at the expected path
    key_path = git.annex.get_annex_key_path(key)
    assert os.path.exists(key_path)
    # 2. Check that the key has the correct registered URL
    # 3. Check that the key has the expected sources
    assert git.annex.is_present(key)
    # 4. Check that the key exists in the shared Dolt branch
    assert git.annex.uuid in get_sources_from_annex_key(dolt.cursor, key)
    assert expected_url in get_urls_from_annex_key(dolt.cursor, key)
    assert get_annex_key_from_url(dolt.cursor, expected_url) == key
    # 5. Check that the key exists in the personl Dolt branch
    with dolt.set_branch(git.annex.uuid):
        assert is_key_present(dolt.cursor, key)
