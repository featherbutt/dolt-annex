#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass

import pytest

from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
async def test_push_local(tmp_path, setup: EnvironmentForTest):

    dataset_name = test_dataset_schema.name
    table_name = test_dataset_schema.tables[0].name
    table_key_column = test_dataset_schema.tables[0].key_columns[0]
    file_column = test_dataset_schema.tables[0].file_column

    remote_name = "test_remote"
    local_name = "__local__"

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
            "--key-columns", record1.table_key,
            "--file-bytes", record1.file_bytes
        ],
        expected_output_contains="Inserted row"
    )
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--key-columns", record2.table_key,
            "--file-bytes", record2.file_bytes
        ],
        expected_output_contains="Inserted row"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "diff",
            "--dataset", dataset_name,
            "--table", table_name,
            "--from", local_name,
            "--to", remote_name,
        ],
        expected_output_equals="""\
added,SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt,('test_key1',)
added,SHA256E-s14--92d7f552b54125f4a8076811c310c671a21b1538842f36afbda91ba7534f21d2.txt,('test_key2',)
"""
    )
