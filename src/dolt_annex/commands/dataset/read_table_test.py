#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.test_util import EnvironmentForTest, run, test_dataset_schema

@pytest.mark.asyncio
async def test_read_table(tmp_path, setup: EnvironmentForTest):
    """Run and validate reading table contents"""
    
    file_bytes = b"new file content"
    file_key = Sha256E.from_bytes(file_bytes, "txt")
    table_key = "table_key"
    
    dataset_name = test_dataset_schema.name
    table_name = test_dataset_schema.tables[0].name
    table_key_column = test_dataset_schema.tables[0].key_columns[0]
    file_column = test_dataset_schema.tables[0].file_column

    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}={table_key}",
            "--file-bytes", file_bytes
        ],
        expected_output_contains="Inserted row"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name
        ],
        expected_output_equals=f"{{'{table_key_column}': '{table_key}', '{file_column}': '{file_key}'}}\n"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", file_column,
            "--columns", table_key_column
        ],
        expected_output_equals=f"{{'{file_column}': '{file_key}', '{table_key_column}': '{table_key}'}}\n"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", table_key_column,
            "--columns", file_column
        ],
        expected_output_equals=f"{{'{table_key_column}': '{table_key}', '{file_column}': '{file_key}'}}\n"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", table_key_column
        ],
        expected_output_equals=f"{{'{table_key_column}': '{table_key}'}}\n"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", file_column
        ],
        expected_output_equals=f"{{'{file_column}': '{file_key}'}}\n"
    )
