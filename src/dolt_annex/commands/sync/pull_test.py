#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import cast

import pytest

from dolt_annex.filestore.memory import MemoryFS

from dolt_annex.test_util import run, EnvironmentForTest

@pytest.mark.asyncio
async def test_pull_local(tmp_path, setup: EnvironmentForTest):
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--key-columns", "test_key1",
                "--file-bytes", "file_content_1",
                "--repo", "test_remote"],
        expected_output="Inserted row"
    )
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--key-columns", "test_key2",
                "--file-bytes", "file_content_2",
                "--repo", "test_remote"],
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
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--key-columns", "test_key3",
                "--file-bytes", "file_content_3",
                "--repo", "test_remote"],
        expected_output="Inserted row"
    )
    await run(
        args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
        expected_output="Pulled 1 files from remote test_remote"
    )

@pytest.mark.asyncio
async def test_pull_missing_file(tmp_path, setup: EnvironmentForTest):
    # If --ignore-missing is set, missing files should be skipped
    # Otherwise, an error should be raised, and the database should reflect files already pulled
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--key-columns", "test_key1",
                "--file-bytes", "file_content_1",
                "--repo", "test_remote"],
        expected_output="Inserted row"
    )
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--key-columns", "test_key2",
                "--file-bytes", "file_content_2",
                "--repo", "test_remote"],
        expected_output="Inserted row"
    )

    remote_memory_store = cast(MemoryFS, setup.remote_file_store.file_store)
    del remote_memory_store.files[b"SHA256E-s14--92d7f552b54125f4a8076811c310c671a21b1538842f36afbda91ba7534f21d2.txt"]

    await run(
        args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
        expected_exception=FileNotFoundError
    )

    # Assert that the first file was still pulled
    local_memory_store = cast(MemoryFS, setup.local_file_store.file_store)
    assert local_memory_store.files[b"SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt"] == b"file_content_1"

    # Assert that the record was added to the local database
    await run(
        args=["dolt-annex", "dataset", "read-table", "--dataset", "test", "--table-name", "test_table"],
        expected_output="SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt, test_key1"
    )

@pytest.mark.asyncio
async def test_file_already_in_local_filestore(tmp_path):
    # Sometimes we may have files already in the local filestore, because the file is in a different dataset.
    # In this case, we don't need to copy the file, but we do need to record that we have a copy of it for this dataset.
    # We test that we don't copy the file by altering it in the remote before the pull
    async with setup(
        tmp_path,
        local_files=[b"file_content_1"],
    ) as (local_filestore, remote_filestore):
        await run(
            args=["dolt-annex", "dataset", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key1",
                  "--file-bytes", "file_content_1",
                  "--repo", "test_remote",
                  "--extension", "",
                ],
            expected_output="Inserted row"
        )

        remote_memory_store = cast(MemoryFS, remote_filestore.file_store)
        remote_memory_store.files[b"SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884"] = b"modified_content"

        # Assert that the local db does not contain any records.
        await run(
            args=["dolt-annex", "dataset", "read-table", "--dataset", "test", "--table-name", "test_table"],
            expected_output_does_not_contain="SHA256E"
        )

        await run(
            args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
        )

        # Assert that the local db now has a record
        await run(
            args=["dolt-annex", "dataset", "read-table", "--dataset", "test", "--table-name", "test_table"],
            expected_output="SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884, test_key1"
        )

        # Assert that the file in the local filestore was not modified
        local_memory_store = cast(MemoryFS, local_filestore.file_store)
        assert local_memory_store.files[b"SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884"] == b"file_content_1"

