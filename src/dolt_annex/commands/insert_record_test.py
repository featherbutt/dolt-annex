#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib

import pytest

from dolt_annex.datatypes.common import TableRow
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.cas import ContentAddressableStorage, maybe_await
from dolt_annex.table import Dataset
from dolt_annex.test_util import run, setup, local_uuid, test_config, test_dataset_schema

@pytest.mark.asyncio
async def test_insert_record(tmp_path):
    local_file_store, remote_file_store = await setup(tmp_path)
    async with local_file_store.open(test_config):
        await do_test_insert_record(tmp_path, local_file_store)

async def do_test_insert_record(tmp_path, local_file_store: ContentAddressableStorage):
    """Run and validate pushing content files to a remote"""

    key = Sha256e.from_bytes(b"new file content", "txt")
    with contextlib.chdir(tmp_path):
        await run(
            args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "new file content"],
            expected_output="Inserted row"
        )
  
        assert await maybe_await(local_file_store.file_store.exists(key))
        async with local_file_store.file_store.with_file_object(key) as file_obj:
            content = await maybe_await(file_obj.read())
            assert content == b"new file content"

        async with Dataset.connect(test_config, 100, test_dataset_schema) as dataset:
            assert dataset.get_table("test_table").get_row(local_uuid, TableRow(("test_key",))) == bytes(key).decode('utf-8')

        # TODO: Verify that the record exists in the dataset
