#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
async def test_push_local(tmp_path, setup: EnvironmentForTest):

    dataset_name = test_dataset_schema.name
    table_name = test_dataset_schema.tables[0].name
    table_key_column = test_dataset_schema.tables[0].key_columns[0]
    file_column = test_dataset_schema.tables[0].file_column

    remote_name = "test_remote"

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
            "--remote", remote_name
        ],
        expected_output_contains=f"Pushed 2 files to remote {remote_name}"
    )

    # Pushing again should result in no files being pushed
    await run(
        args=[
            "dolt-annex", "push",
            "--dataset", dataset_name,
            "--remote", remote_name
        ],
        expected_output_contains=f"Pushed 0 files to remote {remote_name}"
    )

    # But if we add more files, it should push them
    record3 = Record(table_key="test_key3", file_bytes=b"file_content_3")
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}={record3.table_key}",
            "--file-bytes", record3.file_bytes
        ],
        expected_output_contains="Inserted row"
    )
    await run(
        args=[
            "dolt-annex", "push",
            "--dataset", dataset_name,
            "--remote", remote_name
        ],
        expected_output_contains="Pushed 1 files to remote test_remote"
    )

    # Each file should be present in the remote dataset and the remote filestore.
    records = [record1, record2, record3]
    expected_files_keys = [bytes(Sha256E.from_bytes(record.file_bytes, extension="txt")) for record in records]
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
    
    for record, expected_file_key in zip(records, expected_files_keys):
        await run(
            args=[
                "dolt-annex", "filestore", "export-file",
                expected_file_key,
                "--repo", remote_name
            ],
            expected_output_equals=record.file_bytes.decode('utf-8'),
        )