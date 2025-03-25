#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
import os
import shutil
from typing import Dict

from annex import AnnexCache, GitAnnexSettings
from bup.repo import LocalRepo
from bup_ext.bup_ext import CommitMetadata
from commands.import_command import ImportConfig, do_import
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import importers
import move_functions
from tests.setup import setup,  base_config
from type_hints import AnnexKey
from db import get_annex_key_from_url, get_sources_from_annex_key, get_urls_from_annex_key, is_key_present

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")

def key_from_bytes(data: bytes, extension: str) -> AnnexKey:
    data_hash = hashlib.sha256(data).hexdigest()
    return AnnexKey(f"SHA256E-s{len(data)}--{data_hash}.{extension}")

def test_import_with_prefix_url(tmp_path):
    """Test importing with a url determined by the file path"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://prefix/import_data/00/12/34/56/78/591785b794601e212b260e25925636fd.e621.txt",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://prefix/import_data/08/76/54/32/10/b1946ac92492d2347c6235b4d2611184.e621.txt",
    }
    def importer_factory(downloader: GitAnnexDownloader) -> importers.Importer:
        return importers.DirectoryImporter("https://prefix")
    do_test_import(tmp_path, importer_factory, expected_urls)

def test_import_e621(tmp_path):
    """Test importing with a url determined by the md5 hash of the file"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://static1.e621.net/data/59/17/591785b794601e212b260e25925636fd.txt",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://static1.e621.net/data/b1/94/b1946ac92492d2347c6235b4d2611184.txt",
    }
    def importer_factory(downloader: GitAnnexDownloader) -> importers.Importer:
        return importers.MD5Importer()
    do_test_import(tmp_path, importer_factory, expected_urls)

def test_import_falr(tmp_path):
    """Test importing with a url determined by selecting from the Dolt database"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://d.furaffinity.net/art/denalilobita/1742711834/1742711834.denalilobita_transdaffy.png",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://d.furaffinity.net/art/detergentt/1742713111/1742713111.detergentt_pulchra_headshot_18mar2025.png",
    }
    def importer_factory(downloader: GitAnnexDownloader) -> importers.Importer:
        dolt = downloader.dolt_server
        dolt.execute("""CREATE TABLE `filenames` (
  `source` enum('archiveofourown.org','furaffinity.net') NOT NULL,
  `id` int NOT NULL,
  `updated` date NOT NULL,
  `part` int NOT NULL,
  `url` varchar(200),
  `filename` varchar(200),
  PRIMARY KEY (`source`,`id`,`updated`,`part`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin""", [])
        dolt.executemany("INSERT INTO `filenames` VALUES (%s, %s, %s, %s, %s, %s);", [
            ['furaffinity.net', 12345678, '2021-01-01', 1, 'https://d.furaffinity.net/art/denalilobita/1742711834/1742711834.denalilobita_transdaffy.png', '1742711834.denalilobita_transdaffy.png'],
            ['furaffinity.net', 876543210, '2021-01-01', 1, 'https://d.furaffinity.net/art/detergentt/1742713111/1742713111.detergentt_pulchra_headshot_18mar2025.png', '1742713111.detergentt_pulchra_headshot_18mar2025.png'],
        ])
        return importers.FALRImporter(downloader.dolt_server, "dolt", "main")
    do_test_import(tmp_path, importer_factory, expected_urls)

def do_test_import(tmp_path, importer_factory, expected_urls):
    """Run and validate the importer"""
    setup(tmp_path)
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
            importer = importer_factory(downloader)
            do_import(import_config, downloader, importer, ["import_data"])
        validate_import(downloader, expected_urls)

def validate_import(downloader: GitAnnexDownloader, expected_urls: Dict[str, str]):
    """Check that the imported files are present in the annex and the Dolt database"""
    print(os.path.curdir)
    assert os.path.exists("git/annex")
    file_count = 0
    for root, _, files in os.walk(import_directory):
        for file in files:
            file_count += 1
            extension = file.split(".")[-1]
            with open(os.path.join(root, file), "rb") as f:
                key = key_from_bytes(f.read(), extension)
            assert_key(downloader.git, downloader.dolt_server, key, expected_urls[file])

    assert file_count == len(expected_urls)

def assert_key(git: Git, dolt: DoltSqlServer, key: AnnexKey, expected_url: str):
    """Assert that the key and its associated data is present in the annex and the Dolt database"""
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
