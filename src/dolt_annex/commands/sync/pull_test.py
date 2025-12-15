#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import cast

import pytest

from dolt_annex.filestore.memory import MemoryFS
from dolt_annex.test_util import run, setup

@pytest.mark.asyncio
async def test_pull_local(tmp_path):
    async with setup(tmp_path):
        await run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key1",
                  "--file-bytes", "file_content_1",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )
        await run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key2",
                  "--file-bytes", "file_content_2",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )

        await run(
            args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
            expected_output="Pulled 2 files from remote test_remote"
        )

        # Pulling again should result in no files being pulled
        await run(
            args=["dolt-annex", "pull","--dataset", "test", "--remote", "test_remote"],
            expected_output="Pulled 0 files from remote test_remote"
        )

        # But if we add more files, it should push them
        await run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key3",
                  "--file-bytes", "file_content_3",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )
        await run(
            args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
            expected_output="Pulled 1 files from remote test_remote"
        )

@pytest.mark.asyncio
async def test_pull_missing_file(tmp_path):
    # If --ignore-missing is set, missing files should be skipped
    # Otherwise, an error should be raised, and the database should reflect files already pulled
    async with setup(tmp_path) as (local_filestore, remote_filestore):
        await run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key1",
                  "--file-bytes", "file_content_1",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )
        await run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key2",
                  "--file-bytes", "file_content_2",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )

        remote_memory_store = cast(MemoryFS, remote_filestore.file_store)
        del remote_memory_store.files[b"SHA256E-s14--92d7f552b54125f4a8076811c310c671a21b1538842f36afbda91ba7534f21d2.txt"]

        await run(
            args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
            expected_exception=FileNotFoundError
        )

        # Assert that the first file was still pulled
        local_memory_store = cast(MemoryFS, local_filestore.file_store)
        assert local_memory_store.files[b"SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt"] == b"file_content_1"

        # Assert that the record was added to the local database
        await run(
            args=["dolt-annex", "read-table", "--dataset", "test", "--table-name", "test_table"],
            expected_output="SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt, test_key1"
        )
