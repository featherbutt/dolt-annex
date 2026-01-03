#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.cas import maybe_await
from dolt_annex.test_util import EnvironmentForTest, run

@pytest.mark.asyncio
async def test_insert_record(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""
    local_file_store = setup.local_file_store.file_store

    key = Sha256e.from_bytes(b"new file content", "txt")
    await run(
        args=["dolt-annex", "filestore", "insert-file", "--file-bytes", "new file content"],
        expected_output="Inserted file with key"
    )

    assert await maybe_await(local_file_store.exists(key))
    async with local_file_store.with_file_object(key) as file_obj:
        content = await maybe_await(file_obj.read())
        assert content == b"new file content"
