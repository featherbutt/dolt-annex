#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
async def test_delete_redundant_files(tmp_path, setup: EnvironmentForTest):

    dataset_name = test_dataset_schema.name
    table_name = test_dataset_schema.tables[0].name
    table_key_column = test_dataset_schema.tables[0].key_columns[0]
    file_column = test_dataset_schema.tables[0].file_column

    remote_name_1 = "test_remote_1"
    remote_name_2 = "test_remote_2"

    @dataclass
    class Record:
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

    # Pruning delete_redundant_files should remove only the file that was pushed to both remotes
    await run(
        args=[
            "dolt-annex", "script", "delete_redundant_files"
            "--delete-from", setup.local_repo.name,
            "--if-in", remote_name_1,
            "--if-in", remote_name_2
            "--dataset", dataset_name,
        ],
    )

    # Each file should be present in the remote dataset and the remote filestore.
    records = [record1, record2]
    expected_files_keys = [bytes(Sha256E.from_bytes(record.file_bytes, extension="txt")) for record in records]
    for remote_name in [remote_name_1, remote_name_2]:
        await run(
            args=[
                "dolt-annex", "dataset", "read-table",
                "--dataset", dataset_name,
                "--table-name", table_name,
                "--columns", file_column,
                "--columns", table_key_column,
                "--repo", remote_name
            ],
            expected_output_equals='\n'.join(json.dumps({file_column: expected_files_keys[i].decode('utf-8'), table_key_column: record.table_key}) for i, record in enumerate(records))+'\n',
        )
        await run(
            args=[
                "dolt-annex", "filestore", "export-file",
                "--dataset", dataset_name,
                "--table-name", table_name,
                "--columns", file_column,
                "--columns", table_key_column,
                "--repo", remote_name
            ],
            expected_output_equals='\n'.join(json.dumps({file_column: expected_files_keys[i].decode('utf-8'), table_key_column: record.table_key}) for i, record in enumerate(records))+'\n',
        )
        
    
    # TODO: record1 should be missing from the local datastore, while the others should be present