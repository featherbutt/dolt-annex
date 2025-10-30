#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
from pathlib import Path
import shutil
from typing import override
from uuid import UUID

from typing_extensions import Dict, Optional

from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.table import Dataset, FileTable
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.dolt import DoltSqlServer
from dolt_annex import importers, move_functions
from dolt_annex.datatypes import AnnexKey, TableRow
from dolt_annex.datatypes.table import FileTableSchema
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore import FileStore


from tests.setup import setup_file_remote, base_config

class TestImporter(importers.ImporterBase):
    @override
    def key_columns(self, path: Path) -> Optional[TableRow]:
        sid = int(''.join(path.parts[-6:-1]))
        return TableRow(("furaffinity.net", sid, '2021-01-01', 1))
    
    @override
    def table_name(self, path: Path) -> str:
        return "submissions"
    
import_config = ImportConfig(
    batch_size = 10,
    move_function = move_functions.move,
    follow_symlinks = False,
    file_key_type= Sha256e
)

import_directory = Path(__file__).parent / "import_data"
config_directory = Path(__file__).parent / "config"

def test_import_with_prefix_url(tmp_path):
    """Test importing with a url determined by the file path"""
    expected_urls = {
        "591785b794601e212b260e25925636fd.e621.txt": TableRow(("https://prefix/import_data/00/12/34/56/78/591785b794601e212b260e25925636fd.e621.txt",)),
        "b1946ac92492d2347c6235b4d2611184.e621.txt": TableRow(("https://prefix/import_data/08/76/54/32/10/b1946ac92492d2347c6235b4d2611184.e621.txt",)),
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": TableRow(("https://prefix/import_data/00/12/34/56/90/d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt",)),
    }
    def importer_factory() -> importers.ImporterBase:
        return importers.DirectoryImporter("urls", "https://prefix")
    do_test_import(tmp_path, "urls", importer_factory, expected_urls)

def test_import_e621(tmp_path):
    """Test importing with a url determined by the md5 hash of the file"""
    expected_rows = {
        "591785b794601e212b260e25925636fd.e621.txt": TableRow(("591785b794601e212b260e25925636fd",)),
        "b1946ac92492d2347c6235b4d2611184.e621.txt": TableRow(("b1946ac92492d2347c6235b4d2611184",)),
        "d8e8fca2dc0f896fd7cb4cb0031ba249.e621.txt": TableRow(("d8e8fca2dc0f896fd7cb4cb0031ba249",)),
    }
    def importer_factory() -> importers.ImporterBase:
        return importers.MD5Importer("urls")
    do_test_import(tmp_path, "urls", importer_factory, expected_rows)

def do_test_import(tmp_path_: str, table_name: str, importer_factory, expected_rows: Dict[str, TableRow]):
    """Run and validate the importer"""
    tmp_path = Path(tmp_path_)
    print(f"Using temporary path {tmp_path}")
    setup_file_remote(tmp_path)
    shutil.copytree(import_directory, tmp_path / "import_data")
    shutil.copy(config_directory / "submissions.dataset", tmp_path / "submissions.dataset")
    shutil.copy(config_directory / "urls.dataset", tmp_path / "urls.dataset")

    dataset_schema = DatasetSchema.must_load(table_name)

    with (
        DoltSqlServer(base_config.dolt.dolt_dir, base_config.dolt.db_name, base_config.dolt.connection, base_config.dolt.spawn_dolt_server) as dolt_server,
    ):
        with Dataset(base_config, dolt_server, dataset_schema, False, import_config.batch_size) as dataset:
            table = dataset.get_table(table_name)
            importer = importer_factory()
            do_import(base_config.get_filestore(), base_config.get_uuid(), import_config, dataset, importer, ["import_data"])
        validate_import(base_config.get_filestore(), base_config.get_uuid(), import_config.file_key_type, table, expected_rows)

def validate_import(file_store: FileStore, repo_uuid: UUID, file_key_type: FileKeyType, downloader: FileTable, expected_rows: Dict[str, TableRow]):
    """Check that the imported files are present in the annex and the Dolt database"""
    print(os.path.curdir)
    file_count = 0
    for root, _, files in os.walk(import_directory):
        root_path = Path(root)
        for file in files:
            file_count += 1
            key = file_key_type.from_file(root_path / file)
            print(f"Validating {file} with key {key}")
            assert file_store.exists(key)
            assert_submission_id(downloader.dolt, repo_uuid, downloader.schema, key, expected_rows[file])

    assert file_count == len(expected_rows)

def get_annex_key_from_submission_id(dolt: DoltSqlServer, row: TableRow, uuid: UUID, table: FileTableSchema) -> Optional[str]:
    '''Get the annex key for a given submission ID'''
    res = dolt.query(f"SELECT `{table.file_column}` FROM `{dolt.db_name}/{uuid}-{table.name}`.{table.name} WHERE {' AND '.join(f'`{col}` = %s' for col in table.key_columns)}",
                   row)
    for row in res:
        return row[0]
    return None

def assert_submission_id(dolt: DoltSqlServer, repo_uuid: UUID, table_schema: FileTableSchema, key: AnnexKey, expected_submission_id: TableRow):
    """Assert that the key and its associated submission ID is present in the annex and the Dolt database"""
    # 4. Check that the key exists in the personal Dolt branch
    assert get_annex_key_from_submission_id(dolt, expected_submission_id, repo_uuid, table_schema) == key

