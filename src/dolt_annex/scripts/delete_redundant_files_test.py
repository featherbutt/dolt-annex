#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json
import uuid

import fs.memoryfs
import pytest

from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys import Sha256E
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
async def test_delete_redundant_files(tmp_path, setup: EnvironmentForTest):

    dataset_name = test_dataset_schema.name
    table_name = test_dataset_schema.tables[0].name
    table_key_column = test_dataset_schema.tables[0].key_columns[0]
    file_column = test_dataset_schema.tables[0].file_column

    remote1 = RepoModel(
        name="test_remote_1",
        uuid=uuid.uuid4(),
        filestore=AnnexFSModel(root=fs.memoryfs.MemoryFS()),
        key_format=Sha256E,
        alternate_key_formats=[],
    )

    remote2 = RepoModel(
        name="test_remote_2",
        uuid=uuid.uuid4(),
        filestore=AnnexFSModel(root=fs.memoryfs.MemoryFS()),
        key_format=Sha256E,
        alternate_key_formats=[],
    )

    remote_name_1 = "test_remote_1"
    remote_name_2 = "test_remote_2"

    local_uuid = setup.local_repo.uuid

    @dataclass
    class Record:
        def __init__(self, table_key: str, file_bytes: bytes):
             self.file_key = Sha256E.from_bytes(file_bytes, extension="txt")
             self.table_key = table_key
             self.file_bytes = file_bytes

        file_key: FileKey
        table_key: str
        file_bytes: bytes

    record1 = Record(table_key="test_key1", file_bytes=b"file_content_1")
    record2 = Record(table_key="test_key2", file_bytes=b"file_content_2")

    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}={record1.table_key}",
            "--file-bytes", record1.file_bytes
        ],
        expected_output_contains="Inserted row"
    )

    await run(
        args=[
            "dolt-annex", "push",
            "--dataset", dataset_name,
            "--remote", remote_name_1
        ],
        expected_output_contains=f"Pushed 1 files to remote {remote_name_1}"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}={record2.table_key}",
            "--file-bytes", record2.file_bytes
        ],
        expected_output_contains="Inserted row"
    )

    await run(
        args=[
            "dolt-annex", "push",
            "--dataset", dataset_name,
            "--remote", remote_name_2
        ],
        expected_output_contains=f"Pushed 2 files to remote {remote_name_2}"
    )

    # Pruning delete_redundant_files should remove only the file that was pushed to both remotes
    await run(
        args=[
            "dolt-annex", "script", "delete_redundant_files", "--",
            "--table", table_name,
            "--delete-from", setup.local_repo.name,
            "--if-in", remote_name_1,
            "--if-in", remote_name_2,
            "--dataset", dataset_name,
        ],
    )

    await run(
        args=["dolt-annex", "filestore", "whereis", bytes(record1.file_key).decode('utf-8')],
        expected_output_contains=f'[{{"name": "{remote_name_1}", "uuid": "{str(remote1.uuid)}"}}, {{"name": "{remote_name_2}", "uuid": "{str(remote2.uuid)}"}}]'
    )

    await run(
        args=["dolt-annex", "filestore", "whereis", bytes(record2.file_key).decode('utf-8')],
        expected_output_contains=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}, {{"name": "{remote_name_2}", "uuid": "{str(remote2.uuid)}"}}]'
    )

    # Each file should be present in the remote dataset and the remote filestore.
    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", file_column,
            "--columns", table_key_column,
            "--repo", setup.local_repo.name
        ],
        expected_output_equals='\n'.join(json.dumps({file_column: str(record.file_key), table_key_column: record.table_key}) for record in [record2])+'\n',
    )

    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", file_column,
            "--columns", table_key_column,
            "--repo", remote_name_1
        ],
        expected_output_equals='\n'.join(json.dumps({file_column: str(record.file_key), table_key_column: record.table_key}) for record in [record1])+'\n',
    )
    
    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", file_column,
            "--columns", table_key_column,
            "--repo", remote_name_2
        ],
        expected_output_equals='\n'.join(json.dumps({file_column: str(record.file_key), table_key_column: record.table_key}) for record in [record1, record2])+'\n',
    )