#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import contextlib
from pathlib import Path
import uuid

from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.remote import Repo
from dolt_annex.file_keys.base import FileKey
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.cas import ContentAddressableStorage, maybe_await
from dolt_annex.table import Dataset
from dolt_annex.commands.push import push_dataset
from dolt_annex.test_util import run, setup, local_uuid, test_config, test_dataset_schema

files: dict[FileKey, bytes] = {
    Sha256e(b"SHA256E-s5--2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.txt"): b"hello",
    Sha256e(b"SHA256E-s5--486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7.txt"): b"world",
}

def test_insert_record(tmp_path):
    local_file_store, remote_file_store = asyncio.run(
        setup(tmp_path, local_files=[], remote_files=[b"hello", b"world"]))
    do_test_insert_record(tmp_path, local_file_store)

def do_test_insert_record(tmp_path, local_file_store: ContentAddressableStorage):
    """Run and validate pushing content files to a remote"""

    key = Sha256e.from_bytes(b"new file content", "txt")
    with contextlib.chdir(tmp_path):
        run(
            args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "new file content"],
            expected_output="Inserted row"
        )
        async def verify():
            assert await maybe_await(local_file_store.file_store.exists(key))
            async with local_file_store.file_store.with_file_object(key) as file_obj:
                content = await maybe_await(file_obj.read())
                assert content == b"new file content"

            async with Dataset.connect(test_config, 100, test_dataset_schema) as dataset:
                assert dataset.get_table("test_table").get_row(local_uuid, TableRow(("test_key",))) == bytes(key).decode('utf-8')

        asyncio.run(verify())
        # TODO: Verify that the record exists in the dataset

async def push_and_verify(dataset: Dataset, file_remote: Repo, local_uuid: uuid.UUID, remote_file_store: FileStore, local_file_store: FileStore):
    files_pushed = await push_dataset(dataset, local_uuid, file_remote, remote_file_store, local_file_store, [], limit=None)
    await dataset.flush()

    for key in files_pushed:
        assert await maybe_await(remote_file_store.exists(key)), f"Remote filestore missing pushed file {key}"
    # TODO: Test that the branches are correct.

    return len(files_pushed)
