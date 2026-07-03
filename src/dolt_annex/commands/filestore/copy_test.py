#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.test_util import EnvironmentForTest, run

# Test with named files
# Test with --all
# Test with non-content-addredded filestores

@pytest.mark.asyncio
async def test_copy(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""
    key = await setup.local_repo.filestore.put_file_bytes(b"new file content")

    await setup.local_repo.filestore.put_file_bytes(b"new file content", key)
    await run(
        args=["dolt-annex", "filestore", "copy", "--from", setup.local_repo.name, "--to", setup.remote_repo.name, str(key)],
    )

    assert await setup.remote_repo.filestore.exists(key)

@pytest.mark.asyncio
async def test_copy_all(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""
    key = await setup.local_repo.filestore.put_file_bytes(b"new file content")

    await setup.local_repo.filestore.put_file_bytes(b"new file content", key)
    await run(
        args=["dolt-annex", "filestore", "copy", "--from", setup.local_repo.name, "--to", setup.remote_repo.name, "--all"],
    )

    assert await setup.remote_repo.filestore.exists(key)

@pytest.mark.asyncio
@pytest.mark.parametrize("is_content_addressed", [False])
async def test_copy_non_content_addressed(tmp_path, setup: EnvironmentForTest):
    key = Sha256E.from_bytes(b"new file content", "txt")
    await setup.local_repo.filestore.put_file_bytes(b"pointer-to-content", key)
    await run(
        args=["dolt-annex", "filestore", "copy", "--from", setup.local_repo.name, "--to", setup.remote_repo.name, "--all"],
    )
    assert await setup.remote_repo.filestore.exists(key)
    assert await setup.remote_repo.filestore.file_store.get_file_bytes(key) == b"pointer-to-content"
 