#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.util.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
async def test_sync_from_and_to(tmp_path, setup: EnvironmentForTest):

    dataset_name = test_dataset_schema.name
    table_name = test_dataset_schema.tables[0].name
    table_key_column = test_dataset_schema.tables[0].key_columns[0]

    remote_name = "test_remote"

    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}=local_key",
            "--file-bytes", b"local_content",
        ],
        expected_output_contains="Inserted row"
    )

    # The default repo is the source, so this is equivalent to a push.
    await run(
        args=[
            "dolt-annex", "sync",
            "--dataset", dataset_name,
            "--to", remote_name
        ],
        expected_output_contains=f"Synced 1 files from default repo to {remote_name}"
    )

    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}=remote_key",
            "--file-bytes", b"remote_content",
            "--repo", remote_name,
        ],
        expected_output_contains="Inserted row"
    )

    # The default repo is the destination, so this is equivalent to a pull.
    await run(
        args=[
            "dolt-annex", "sync",
            "--dataset", dataset_name,
            "--from", remote_name
        ],
        expected_output_contains=f"Synced 1 files from {remote_name} to default repo"
    )


@pytest.mark.asyncio
async def test_sync_requires_different_repos(tmp_path, setup: EnvironmentForTest):

    dataset_name = test_dataset_schema.name

    await run(
        args=[
            "dolt-annex", "sync",
            "--dataset", dataset_name,
        ],
        expected_output_contains="The source and destination repos must be different",
        expected_error_code=1,
    )
