#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json
from typing_extensions import cast

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.memory import MemoryFS

from dolt_annex.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
async def test_pull_local(tmp_path, setup: EnvironmentForTest):

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
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", dataset_name,
                "--table-name", table_name,
                "--value", f"{table_key_column}={record1.table_key}",
                "--file-bytes", record1.file_bytes,
                "--repo", remote_name],
        expected_output_contains="Inserted row"
    )
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", dataset_name,
                "--table-name", table_name,
                "--value", f"{table_key_column}={record2.table_key}",
                "--file-bytes", record2.file_bytes,
                "--repo", remote_name],
        expected_output_contains="Inserted row"
    )

    await run(
        args=[
            "dolt-annex", "pull",
            "--dataset", dataset_name,
            "--remote", remote_name
        ],
        expected_output_contains=f"Pulled 2 files from remote {remote_name}"
    )

    # Pulling again should result in no files being pulled
    await run(
        args=[
            "dolt-annex", "pull",
            "--dataset", dataset_name,
            "--remote", remote_name
        ],
        expected_output_contains=f"Pulled 0 files from remote {remote_name}"
    )

    # But if we add more files, it should pull them
    record3 = Record(table_key="test_key3", file_bytes=b"file_content_3")
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--value", f"{table_key_column}={record3.table_key}",
            "--file-bytes", record3.file_bytes,
            "--repo", remote_name
        ],
        expected_output_contains="Inserted row"
    )
    await run(
        args=[
            "dolt-annex", "pull",
            "--dataset", dataset_name,
            "--remote", remote_name
        ],
        expected_output_contains=f"Pulled 1 files from remote {remote_name}"
    )

    # Each file should be present in the local dataset and the local filestore.
    records = [record1, record2, record3]
    expected_files_keys = [bytes(Sha256E.from_bytes(record.file_bytes, extension="txt")) for record in records]
    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", dataset_name,
            "--table-name", table_name,
            "--columns", file_column,
            "--columns", table_key_column
        ],
        expected_output_equals='\n'.join(json.dumps({file_column: expected_files_keys[i].decode('utf-8'), table_key_column: record.table_key}) for i, record in enumerate(records))+'\n',
    )
    
    for record, expected_file_key in zip(records, expected_files_keys):
        await run(
            args=[
                "dolt-annex", "filestore", "export-file",
                expected_file_key,
            ],
            expected_output_equals=record.file_bytes.decode('utf-8'),
        )

@pytest.mark.asyncio
async def test_pull_missing_file(tmp_path, setup: EnvironmentForTest):
    # If --ignore-missing is set, missing files should be skipped
    # Otherwise, an error should be raised, and the database should reflect files already pulled
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--value", "path=test_key1",
                "--file-bytes", "file_content_1",
                "--repo", "test_remote"],
        expected_output_contains="Inserted row"
    )
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--value", "path=test_key2",
                "--file-bytes", "file_content_2",
                "--repo", "test_remote"],
        expected_output_contains="Inserted row"
    )

    remote_memory_store = cast(MemoryFS, setup.remote_repo.filestore.file_store)
    del remote_memory_store.files[b"SHA256E-s14--92d7f552b54125f4a8076811c310c671a21b1538842f36afbda91ba7534f21d2.txt"]

    await run(
        args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
        expected_exception=FileNotFoundError
    )

    # Assert that the first file was still pulled
    local_memory_store = cast(MemoryFS, setup.local_repo.filestore.file_store)
    assert local_memory_store.files[b"SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt"] == b"file_content_1"

    # Assert that the record was added to the local database
    await run(
        args=["dolt-annex", "dataset", "read-table", "--dataset", "test", "--table-name", "test_table"],
        expected_output_contains='{"path": "test_key1", "file_key": "SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884.txt"}'
    )

@pytest.mark.asyncio
async def test_file_already_in_local_filestore(tmp_path, setup: EnvironmentForTest):
    # Sometimes we may have files already in the local filestore, because the file is in a different dataset.
    # In this case, we don't need to copy the file, but we do need to record that we have a copy of it for this dataset.
    # We test that we don't copy the file by altering it in the remote before the pull
    await run(
        args=["dolt-annex", "filestore", "insert-file", "--repo", "__local__", "--file-bytes", "file_content_1", "--extension", ""],
        expected_output_contains="Inserted file with key"
    )
    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", "test",
                "--table-name", "test_table",
                "--value", "path=test_key1",
                "--file-bytes", "file_content_1",
                "--repo", "test_remote",
                "--extension", "",
            ],
        expected_output_contains="Inserted row"
    )

    remote_memory_store = cast(MemoryFS, setup.remote_repo.filestore.file_store)
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
        expected_output_contains='{"path": "test_key1", "file_key": "SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884"}'
    )

    # Assert that the file in the local filestore was not modified
    local_memory_store = cast(MemoryFS, setup.local_repo.filestore.file_store)
    assert local_memory_store.files[b"SHA256E-s14--f17ac4b5e53ad9ea8b33b4c7914abb234e57c281c13ba580098dbb5d10ae0884"] == b"file_content_1"

@pytest.mark.asyncio
@pytest.mark.parametrize("filestore_config", [{"verify_existing_files_on_write": True}])
async def test_pull_overwrites_corrupt_local_file(tmp_path, setup: EnvironmentForTest):
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
    record1_key = Sha256E.from_bytes(record1.file_bytes, extension="txt")

    await run(
        args=["dolt-annex", "dataset", "insert-record",
                "--dataset", dataset_name,
                "--table-name", table_name,
                "--value", f"{table_key_column}={record1.table_key}",
                "--file-bytes", record1.file_bytes,
                "--repo", remote_name],
        expected_output_contains="Inserted row"
    )

    await setup.local_repo.filestore.file_store.put_file_bytes(b"corrupted_content", file_key=record1_key)

    await run(
        args=[
            "dolt-annex", "pull",
            "--dataset", dataset_name,
            "--remote", remote_name
        ],
        expected_output_contains=f"Pulled 1 files from remote {remote_name}"
    )

    assert await setup.local_repo.filestore.file_store.get_file_bytes(record1_key) == record1.file_bytes