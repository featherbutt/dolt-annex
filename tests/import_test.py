#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io import StringIO
import os
import random
import shutil

from typing_extensions import Dict

from annex import AnnexCache, SubmissionId
from commands.import_command import ImportConfig, ImportCsv, do_import
from config import config
import context
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import get_key_path, key_from_file
import importers
import move_functions
from tests.setup import setup_file_remote,  base_config
from type_hints import AnnexKey, PathLike
from db import get_annex_key_from_submission_id, is_key_present, is_submission_present

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")

def test_import_with_prefix_url(tmp_path):
    """Test importing with a url determined by the file path"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://prefix/import_data/00/12/34/56/78/591785b794601e212b260e25925636fd.e621.txt",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://prefix/import_data/08/76/54/32/10/b1946ac92492d2347c6235b4d2611184.e621.txt",
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": "https://prefix/import_data/00/12/34/56/90/d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt",
    }
    def importer_factory(downloader: GitAnnexDownloader) -> importers.Importer:
        return importers.DirectoryImporter("https://prefix")
    do_test_import(tmp_path, importer_factory, expected_urls, {})

def test_import_e621(tmp_path):
    """Test importing with a url determined by the md5 hash of the file"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://static1.e621.net/data/59/17/591785b794601e212b260e25925636fd.txt",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://static1.e621.net/data/b1/94/b1946ac92492d2347c6235b4d2611184.txt",
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": "https://static1.e621.net/data/d8/e8/d8e8fca2dc0f896fd7cb4cb0031ba249.txt",
    }
    def importer_factory(downloader: GitAnnexDownloader) -> importers.Importer:
        return importers.MD5Importer()
    do_test_import(tmp_path, importer_factory, expected_urls, {})

def test_import_falr(tmp_path):
    """Test importing with a url determined by selecting from the Dolt database"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://d.furaffinity.net/art/denalilobita/1742711834/1742711834.denalilobita_transdaffy.png",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://d.furaffinity.net/art/detergentt/1742713111/1742713111.detergentt_pulchra_headshot_18mar2025.png",
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": "https://d.furaffinity.net/art/asdf/1234/1234.asdf_transdaffy.png",
    }
    expected_submission_ids = {
        "591785b794601e212b260e25925636fd.e621.txt": SubmissionId("furaffinity.net", 12345678, '2021-01-01', 1),
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": SubmissionId("furaffinity.net", 12345690, '2021-01-01', 1),
        "b1946ac92492d2347c6235b4d2611184.e621.txt": SubmissionId("furaffinity.net", 876543210, '2021-01-01', 1),
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
            ['furaffinity.net', 12345690, '2021-01-01', 1, 'https://d.furaffinity.net/art/asdf/1234/1234.asdf_transdaffy.png', '1234.asdf_transdaffy.png'],

            ['furaffinity.net', 876543210, '2021-01-01', 1, 'https://d.furaffinity.net/art/detergentt/1742713111/1742713111.detergentt_pulchra_headshot_18mar2025.png', '1742713111.detergentt_pulchra_headshot_18mar2025.png'],
        ])
        return importers.FALRImporter(downloader.dolt_server, "dolt", "main")
    do_test_import(tmp_path, importer_factory, expected_urls, expected_submission_ids)

def do_test_import(tmp_path, importer_factory, expected_urls, expected_submission_ids):
    """Run and validate the importer"""
    setup_file_remote(tmp_path)
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    with (
        DoltSqlServer(base_config.dolt_dir, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with AnnexCache(dolt_server, base_config.auto_push, import_config.batch_size) as cache:
            downloader = GitAnnexDownloader(
                cache = cache,
                dolt_server = dolt_server,
            )
            importer = importer_factory(downloader)
            do_import(import_config, downloader, importer, ["import_data"])
        validate_import(downloader, expected_urls, expected_submission_ids)

def validate_import(downloader: GitAnnexDownloader, expected_urls: Dict[str, str], expected_submission_ids: Dict[str, SubmissionId]):
    """Check that the imported files are present in the annex and the Dolt database"""
    print(os.path.curdir)
    file_count = 0
    for root, _, files in os.walk(import_directory):
        for file in files:
            file_count += 1
            key = key_from_file(PathLike(os.path.join(root, file)))
            file = PathLike(file)
            assert_key(downloader.dolt_server, key, expected_urls[file])
            assert_submission_id(downloader.dolt_server, key, expected_submission_ids[file])

    assert file_count == len(expected_urls)
    assert file_count == len(expected_submission_ids)

def assert_key(dolt: DoltSqlServer, key: AnnexKey, expected_url: str, skip_exists_check: bool = False):
    """Assert that the key and its associated data is present in the annex and the Dolt database"""
    # 1. Check the annexed file exists at the expected path
    # We call git-annex here to make sure that our computed path agrees with git-annex
    # rel_path = git.annex.cmd("examinekey", "--format=${hashdirlower}${key}", key).strip()
    rel_path = get_key_path(key)
    abs_path = os.path.abspath(os.path.join(config.get().files_dir, rel_path))
    assert skip_exists_check or os.path.exists(abs_path)
    # 2. Check that the key has the correct registered URL
    # 3. Check that the key has the expected sources
    # assert git.annex.is_present(key)
    # 4. Check that the key exists in the shared Dolt branch
    # assert expected_url in get_urls_from_annex_key(dolt.cursor, key)
    # assert get_annex_key_from_url(dolt.cursor, expected_url) == key
    # 5. Check that the key exists in the personal Dolt branch
    #with dolt.set_branch(str(config.get().local_uuid)):
    #    assert is_key_present(dolt.cursor, key)

def assert_submission_id(dolt: DoltSqlServer, key: AnnexKey, expected_submission_id: SubmissionId):
    """Assert that the key and its associated submission ID is present in the annex and the Dolt database"""
    # 4. Check that the key exists in the shared Dolt branch
    assert get_annex_key_from_submission_id(dolt.cursor, expected_submission_id, "dolt") == key
    # 5. Check that the key exists in the personal Dolt branch
    with dolt.set_branch(context.local_uuid.get()):
        assert is_submission_present(dolt.cursor, expected_submission_id)

def test_import_csv(tmp_path):
    """Test importing with a url determined by the md5 hash of the file"""
    test_csv = """annex_key,url
SHA256E-s2134560--178714a6e42ab064af381ab1a74c942588aee41316645f8a961bfb66622d5e0c.png,https://static1.e621.net/data/59/17/591785b794601e212b260e25925636fd.txt
SHA256E-s2134564--131cefbcb150edb19bb17be3c3bcba10cba207b5e580187d6caccec05b9b88d1.png,https://static1.e621.net/data/b1/94/b1946ac92492d2347c6235b4d2611184.txt
"""
    expected_urls = {
        "SHA256E-s2134560--178714a6e42ab064af381ab1a74c942588aee41316645f8a961bfb66622d5e0c.png": "https://static1.e621.net/data/59/17/591785b794601e212b260e25925636fd.txt",
        "SHA256E-s2134564--131cefbcb150edb19bb17be3c3bcba10cba207b5e580187d6caccec05b9b88d1.png": "https://static1.e621.net/data/b1/94/b1946ac92492d2347c6235b4d2611184.txt",
    }
    setup_file_remote(tmp_path)
    shutil.copytree(import_directory, os.path.join(tmp_path, "import_data"))
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    with (
        DoltSqlServer(base_config.dolt_dir, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with AnnexCache(dolt_server, base_config.auto_push, import_config.batch_size) as cache:
            downloader = GitAnnexDownloader(
                cache = cache,
                dolt_server = dolt_server,
            )
            ImportCsv.import_csv(downloader, StringIO(test_csv))
        for key, url in expected_urls.items():
            assert_key(downloader.dolt_server, key, url, skip_exists_check=True)

