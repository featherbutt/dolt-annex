#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.file_keys import Sha256E
from dolt_annex.util.test_util import EnvironmentForTest, run, test_dataset_schema

@pytest.mark.asyncio
async def test_insert_record(tmp_path, setup: EnvironmentForTest):
    """Run and validate inserting content files into a repo"""
    local_file_store = setup.local_repo.file_store

    key = Sha256E.from_bytes(b"new file content", "txt")
    await run(
        args=["dolt-annex", "dataset", "insert-record", "--dataset", "test", "--table-name", "test_table", "--value", "path=test_key", "--file-bytes", "new file content"],
        expected_output_contains="Inserted row"
    )

    assert await local_file_store.exists(key)
    async with local_file_store.with_file_object(key) as file_obj:
        content = await file_obj.read()
        assert content == b"new file content"

    async with (
        DatabaseConnection.open(setup.config) as conn,
        as_acm(conn.open_dataset(test_dataset_schema)) as dataset,
        dataset.with_repo(setup.local_repo.uuid) as dataset_replica,
    ):
        result_row = dataset_replica.get_table("test_table").get_row(filters=[TableFilter("path", "test_key")])
        assert result_row is not None
        assert result_row["file_key"] == bytes(key).decode('utf-8')

