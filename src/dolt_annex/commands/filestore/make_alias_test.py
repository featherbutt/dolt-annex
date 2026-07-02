#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.file_keys.base import MD5HSe, MD5e, Sha256HSe
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.archivefs import ArchiveFSModel
from dolt_annex.filestore.cas import maybe_await
from dolt_annex.filestore.memory import MemoryFS, MemoryFSModel
from dolt_annex.test_util import EnvironmentForTest, run

@pytest.mark.asyncio
@pytest.mark.parametrize("key_format", [Sha256E])
@pytest.mark.parametrize("local_filestore_model", [MemoryFSModel()])
async def test_migrate(tmp_path, setup: EnvironmentForTest):
    """Run and validate that we can make aliases, and that making aliases only rehashes the file contents when necessary"""
    local_file_store = setup.local_repo.filestore.file_store
    assert isinstance(local_file_store, MemoryFS)

    key = Sha256E.from_bytes(b"new file content", "txt")
    assert Sha256HSe.convertable_from(Sha256E)
    sha256_alias_key = Sha256HSe.convert_from(key)
    assert not MD5HSe.convertable_from(Sha256E)
    md5_alias_key = MD5HSe.from_bytes(b"new file content", "txt")

    await run(
        args=["dolt-annex", "filestore", "insert-file", "--file-bytes", "new file content"],
        expected_output_contains="Inserted file with key"
    )

    await run(
        args=["dolt-annex", "filestore", "make-alias", "--key-type", "MD5_HSe", str(key)],
    )

    assert await maybe_await(local_file_store.exists(md5_alias_key))

    # We want to assert that making an alias with the same hash does not rehash the file contents.
    # To do this, we will corrupt the file contents, and then make an alias. If the file contents are rehashed,
    # then the command will fail or create an alias with the wrong key.
    assert bytes(key) in local_file_store.files
    local_file_store.files[bytes(key)] = b"corrupted file content"
    await run(
        args=["dolt-annex", "filestore", "make-alias", "--key-type", "SHA256_HSe", str(key)],
    )
    assert await maybe_await(local_file_store.exists(sha256_alias_key))
