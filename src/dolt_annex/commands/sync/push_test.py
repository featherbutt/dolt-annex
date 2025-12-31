#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib

import pytest

from dolt_annex.test_util import run, EnvironmentForTest

@pytest.mark.asyncio
async def test_push_local(tmp_path, setup: EnvironmentForTest):
    await run(
        args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key1", "--file-bytes", "file_content_1"],
        expected_output="Inserted row"
    )
    await run(
        args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key2", "--file-bytes", "file_content_2"],
        expected_output="Inserted row"
    )

    await run(
        args=["dolt-annex", "push", "--dataset", "test", "--remote", "test_remote"],
        expected_output="Pushed 2 files to remote test_remote"
    )

    # Pushing again should result in no files being pushed
    await run(
        args=["dolt-annex", "push", "--dataset", "test", "--remote", "test_remote"],
        expected_output="Pushed 0 files to remote test_remote"
    )

    # But if we add more files, it should push them
    await run(
        args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key3", "--file-bytes", "file_content_3"],
        expected_output="Inserted row"
    )
    await run(
        args=["dolt-annex", "push", "--dataset", "test", "--remote", "test_remote"],
        expected_output="Pushed 1 files to remote test_remote"
    )
