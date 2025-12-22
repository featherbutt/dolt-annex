#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib

import pytest

from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.test_util import run, setup, local_uuid, test_config

@pytest.mark.asyncio
async def test_whereis(tmp_path):
    async with (
        setup(tmp_path) as (local_file_store, remote_file_store),
        local_file_store.open(test_config)
    ):
        await do_test_whereis(tmp_path, local_file_store)

async def do_test_whereis(tmp_path, local_file_store: ContentAddressableStorage):
    """Run and validate pushing content files to a remote"""

    key = Sha256e.from_bytes(b"new file content", "txt")
    with contextlib.chdir(tmp_path):
        await run(
            args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "new file content"],
            expected_output="Inserted row"
        )

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(key).decode('utf-8'), "--repo", "__local__"],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )
        
        await run(
            args=["dolt-annex", "whereis", "--file-key", "nonexistentkey", "--repo", "__local__"],
            expected_output='[]'
        )