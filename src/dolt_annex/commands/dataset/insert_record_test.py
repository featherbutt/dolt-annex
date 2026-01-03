#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.datatypes.common import TableRow
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.cas import maybe_await
from dolt_annex.table import Dataset
from dolt_annex.test_util import EnvironmentForTest, run, test_config, local_uuid, test_dataset_schema

@pytest.mark.asyncio
async def test_insert_record(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""
    local_file_store = setup.local_file_store.file_store

    key = Sha256e.from_bytes(b"new file content", "txt")
    await run(
        args=["dolt-annex", "dataset", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "new file content"],
        expected_output="Inserted row"
    )

    assert await maybe_await(local_file_store.exists(key))
    async with local_file_store.with_file_object(key) as file_obj:
        content = await maybe_await(file_obj.read())
        assert content == b"new file content"

    async with Dataset.connect(test_config, 100, test_dataset_schema) as dataset:
        assert dataset.get_table("test_table").get_row(local_uuid, TableRow(("test_key",))) == bytes(key).decode('utf-8')

