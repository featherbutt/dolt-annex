#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import random
import shutil
from uuid import UUID

from typing_extensions import Dict, Optional

from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.table import Dataset, FileTable
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.config import get_config
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.file_keys import key_from_file
from dolt_annex.filestore import get_key_path
from dolt_annex import importers, move_functions
from dolt_annex.datatypes import AnnexKey, TableRow, FileTableSchema, DatasetSource, Repo

from tests.setup import setup_file_remote,  base_config

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
    def importer_factory(downloader: FileTable) -> importers.ImporterBase:
        return importers.DirectoryImporter("https://prefix")
    do_test_import(tmp_path, "urls", importer_factory, expected_urls)

def test_import_e621(tmp_path):
    """Test importing with a url determined by the md5 hash of the file"""
    expected_rows = {
        "591785b794601e212b260e25925636fd.e621.txt": "591785b794601e212b260e25925636fd",
        "b1946ac92492d2347c6235b4d2611184.e621.txt": "b1946ac92492d2347c6235b4d2611184",
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": "d8e8fca2dc0f896fd7cb4cb0031ba249",
    }
    def importer_factory(downloader: FileTable) -> importers.ImporterBase:
        return importers.MD5Importer()
    do_test_import(tmp_path, "urls", importer_factory, expected_rows)

def do_test_import(tmp_path_: str, table_name: str, importer_factory, expected_rows: Dict[str, TableRow]):
    """Run and validate the importer"""
    tmp_path = Path(tmp_path_)
    setup_file_remote(tmp_path)
    shutil.copytree(import_directory, tmp_path / "import_data")
    shutil.copy(config_directory / "submissions.dataset", tmp_path / "submissions.dataset")
    shutil.copy(config_directory / "urls.dataset", tmp_path / "urls.dataset")

    dataset = DatasetSchema.must_load(table_name)
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": random.randint(20000, 21000),
    }
    table_settings = DatasetSource(dataset, repo=base_config.local_repo())
    
    local_remote = Repo(
        name="local",
        uuid=base_config.local_uuid,
        files_url=base_config.files_dir.as_posix(),
    )
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
    ):
        with Dataset(dolt_server, table_settings, base_config.auto_push, import_config.batch_size) as downloader:
            table = downloader.get_table(table_name)
            importer = importer_factory(downloader)
            do_import(local_remote, import_config, table, importer, ["import_data"])
        validate_import(table, table_settings, expected_rows)

def validate_import(downloader: FileTable, table_settings: DatasetSource, expected_rows: Dict[str, TableRow]):
    """Check that the imported files are present in the annex and the Dolt database"""
    print(os.path.curdir)
    file_count = 0
    for root, _, files in os.walk(import_directory):
        root_path = Path(root)
        for file in files:
            file_count += 1
            key = key_from_file(root_path / file)
            print(f"Validating {file} with key {key}")
            assert_key(downloader.dolt, key)
            assert_submission_id(downloader.dolt, table_settings, downloader.schema, key, expected_rows[file])

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

def get_annex_key_from_submission_id(dolt: DoltSqlServer, row: TableRow, uuid: UUID, table: FileTableSchema) -> Optional[str]:
    '''Get the annex key for a given submission ID'''
    res = dolt.query(f"SELECT `{table.file_column}` FROM `{dolt.db_name}/{uuid}-{table.name}`.{table.name} WHERE {' AND '.join(f'`{col}` = %s' for col in table.key_columns)}",
                   row)
    for row in res:
        return row[0]
    return None

def assert_submission_id(dolt: DoltSqlServer, table_settings: DatasetSource, table_schema: FileTableSchema, key: AnnexKey, expected_submission_id: TableRow):
    """Assert that the key and its associated submission ID is present in the annex and the Dolt database"""
    # 4. Check that the key exists in the personal Dolt branch
    assert get_annex_key_from_submission_id(dolt, expected_submission_id, table_settings.repo.uuid, table_schema) == key

