#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.test_util import EnvironmentForTest, run, test_dataset_schema

@pytest.mark.asyncio
async def test_remove_record(tmp_path, setup: EnvironmentForTest):
    """Run and validate removing records from a repo"""
    await run(
        args=["dolt-annex", "dataset", "insert-record", "--dataset", "test", "--table-name", "test_table", "--value", "path=test_key", "--file-bytes", "new file content"],
        expected_output_contains="Inserted row"
    )

    await run(
        args=["dolt-annex", "dataset", "insert-record", "--dataset", "test", "--table-name", "test_table", "--value", "path=test_key2", "--file-bytes", "new file content2"],
        expected_output_contains="Inserted row"
    )

    async with (
        as_acm(DatabaseConnection.open(setup.config)) as conn,
        as_acm(conn.open_dataset(test_dataset_schema)) as dataset,
        dataset.with_repo(setup.local_repo.uuid) as repo_dataset,
    ):
        result_rows = list(repo_dataset.get_table("test_table").get_rows())
        assert len(result_rows) == 2

    # Check that only the specified record was removed
    await run(
        args=["dolt-annex", "dataset", "remove-record", "--dataset", "test", "--table-name", "test_table", "--where", "path=test_key"],
        expected_output_contains="Removed row"
    )

    async with (
        as_acm(DatabaseConnection.open(setup.config)) as conn,
        as_acm(conn.open_dataset(test_dataset_schema)) as dataset,
        dataset.with_repo(setup.local_repo.uuid) as repo_dataset,
    ):
        result_rows = list(repo_dataset.get_table("test_table").get_rows())
        assert len(result_rows) == 1
        assert result_rows[0]["path"] == "test_key2"
