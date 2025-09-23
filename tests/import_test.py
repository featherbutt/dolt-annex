#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io import StringIO
import os
from pathlib import Path
import random
import shutil

from typing_extensions import Dict

from annex import AnnexCache, SubmissionId
from commands.import_command import ImportConfig, ImportCsv, do_import
from config import get_config
from dolt import DoltSqlServer
from git import get_key_path, key_from_file
import importers
import move_functions
from table_settings import TableSettings
from tables import FileKeyTable
from tests.setup import setup_file_remote,  base_config
from type_hints import AnnexKey, TableRow
from db import get_annex_key_from_submission_id

import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
)

import_directory = Path(__file__).parent / "import_data"
config_directory = Path(__file__).parent / "config"

def test_import_with_prefix_url(tmp_path):
    """Test importing with a url determined by the file path"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": "https://prefix/import_data/00/12/34/56/78/591785b794601e212b260e25925636fd.e621.txt",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "https://prefix/import_data/08/76/54/32/10/b1946ac92492d2347c6235b4d2611184.e621.txt",
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": "https://prefix/import_data/00/12/34/56/90/d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt",
    }
    def importer_factory(downloader: AnnexCache) -> importers.ImporterBase:
        return importers.DirectoryImporter("https://prefix")
    do_test_import(tmp_path, "urls", importer_factory, expected_urls)

def test_import_e621(tmp_path):
    """Test importing with a url determined by the md5 hash of the file"""
    expected_rows = {
        "591785b794601e212b260e25925636fd.e621.txt": "591785b794601e212b260e25925636fd",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "b1946ac92492d2347c6235b4d2611184",
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": "d8e8fca2dc0f896fd7cb4cb0031ba249",
    }
    def importer_factory(downloader: AnnexCache) -> importers.ImporterBase:
        return importers.MD5Importer()
    do_test_import(tmp_path, "urls", importer_factory, expected_rows)

def test_import_falr(tmp_path):
    """Test importing with a url determined by selecting from the Dolt database"""
    expected_submission_ids = {
        "591785b794601e212b260e25925636fd.e621.txt": TableRow("furaffinity.net", 12345678, '2021-01-01', 1),
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": TableRow("furaffinity.net", 12345690, '2021-01-01', 1),
        "b1946ac92492d2347c6235b4d2611184.e621.txt": TableRow("furaffinity.net", 876543210, '2021-01-01', 1),
    }
    def importer_factory(downloader: AnnexCache) -> importers.ImporterBase:
        dolt = downloader.dolt
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
        return importers.FALRImporter(downloader.dolt, "dolt", "main")
    do_test_import(tmp_path, "submissions", importer_factory, expected_submission_ids)

def do_test_import(tmp_path_: str, table_name: str, importer_factory, expected_rows: Dict[str, TableRow]):
    """Run and validate the importer"""
    tmp_path = Path(tmp_path_)
    setup_file_remote(tmp_path)
    shutil.copytree(import_directory, tmp_path / "import_data")
    shutil.copy(config_directory / "submissions.table", tmp_path / "submissions.table")
    shutil.copy(config_directory / "urls.table", tmp_path / "urls.table")

    table = FileKeyTable.from_name(table_name)
    if not table:
        raise ValueError(f"Table {table_name} not found")
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    table_settings = TableSettings(uuid=get_config().local_uuid, table=table, remote=None)
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with AnnexCache(dolt_server, table, base_config.auto_push, import_config.batch_size) as downloader:
            importer = importer_factory(downloader)
            do_import(import_config, downloader, importer, ["import_data"])
        validate_import(downloader, table_settings, expected_rows)

def validate_import(downloader: AnnexCache, table_settings: TableSettings, expected_rows: Dict[str, TableRow]):
    """Check that the imported files are present in the annex and the Dolt database"""
    print(os.path.curdir)
    file_count = 0
    for root, _, files in os.walk(import_directory):
        root_path = Path(root)
        for file in files:
            file_count += 1
            key = key_from_file(root_path / file)
            assert_key(downloader.dolt, key)
            assert_submission_id(downloader.dolt, table_settings, key, expected_rows[file])

    assert file_count == len(expected_rows)

def assert_key(dolt: DoltSqlServer, key: AnnexKey, skip_exists_check: bool = False):
    """Assert that the key and its associated data is present in the annex and the Dolt database"""
    # 1. Check the annexed file exists at the expected path
    # We call git-annex here to make sure that our computed path agrees with git-annex
    # rel_path = git.annex.cmd("examinekey", "--format=${hashdirlower}${key}", key).strip()
    rel_path = get_key_path(key)
    abs_path = os.path.abspath(os.path.join(get_config().files_dir, rel_path))
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

def assert_submission_id(dolt: DoltSqlServer, table_settings: TableSettings, key: AnnexKey, expected_submission_id: TableRow):
    """Assert that the key and its associated submission ID is present in the annex and the Dolt database"""
    # 4. Check that the key exists in the personal Dolt branch
    assert get_annex_key_from_submission_id(dolt, expected_submission_id, table_settings.uuid, table_settings.table) == key

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
    table = FileKeyTable.from_name("submissions")
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
        AnnexCache(dolt_server, table, base_config.auto_push, import_config.batch_size) as downloader
    ):
        ImportCsv.import_csv(downloader, StringIO(test_csv))
        for key, url in expected_urls.items():
            assert_key(downloader.dolt, key, skip_exists_check=True)

