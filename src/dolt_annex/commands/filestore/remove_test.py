#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.memory import MemoryFS, MemoryFSModel
from dolt_annex.util.test_util import EnvironmentForTest, run

@pytest.mark.asyncio
async def test_remove_file_from_args(setup: EnvironmentForTest):
    local_file_store = setup.local_repo.file_store
    key = Sha256E.from_bytes(b"file to remove", "txt")

    await run(
        args=["dolt-annex", "filestore", "insert-file", "--file-bytes", "file to remove"],
        expected_output_contains="Inserted file with key"
    )
    assert await setup.local_repo.exists(key)

    await run(
        args=["dolt-annex", "filestore", "remove-file", "--repo", "__local__", str(key)],
    )

    assert not await local_file_store.exists(key)

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", [MemoryFSModel()])
async def test_remove_file_from_stdin(setup: EnvironmentForTest):
    local_file_store = setup.local_repo
    assert isinstance(local_file_store.file_store, MemoryFS)
    key = Sha256E.from_bytes(b"file removed via stdin", "txt")

    await run(
        args=["dolt-annex", "filestore", "insert-file", "--file-bytes", "file removed via stdin"],
        expected_output_contains="Inserted file with key"
    )
    assert await local_file_store.exists(key)

    await run(
        args=["dolt-annex", "filestore", "remove-file", "--repo", "__local__"],
        stdin=f"{key}\n",
    )

    assert not await local_file_store.exists(key)

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", [MemoryFSModel()])
async def test_remove_only_affects_named_repo(setup: EnvironmentForTest):
    """Removing a file from one repo must not remove it from another repo."""
    local_file_store = setup.local_repo.file_store
    remote_file_store = setup.remote_repo.file_store
    key = Sha256E.from_bytes(b"in both repos", "txt")

    await run(
        args=["dolt-annex", "filestore", "insert-file", "--file-bytes", "in both repos"],
        expected_output_contains="Inserted file with key"
    )
    await run(
        args=["dolt-annex", "filestore", "insert-file", "--repo", "test_remote", "--file-bytes", "in both repos"],
        expected_output_contains="Inserted file with key"
    )
    assert await local_file_store.exists(key)
    assert await remote_file_store.exists(key)

    await run(
        args=["dolt-annex", "filestore", "remove-file", "--repo", "test_remote", str(key)],
    )

    assert await local_file_store.exists(key)
    assert not await remote_file_store.exists(key)
