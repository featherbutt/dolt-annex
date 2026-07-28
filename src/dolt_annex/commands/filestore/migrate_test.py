#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.archivefs import ArchiveFSModel
from dolt_annex.filestore.cas import maybe_await
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.test_util import EnvironmentForTest, run

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", [AnnexFSModel(root=pathlib.Path('./annex'))])
@pytest.mark.parametrize("remote_filestore_model", [ArchiveFSModel(root=pathlib.Path('./archive'), secondary=MemoryFSModel())])
async def test_migrate(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""
    local_file_store = setup.local_repo.filestore.file_store
    remote_file_store = setup.remote_repo.filestore.file_store

    key = Sha256E.from_bytes(b"new file content", "txt")
    await run(
        args=["dolt-annex", "filestore", "insert-file", "--file-bytes", "new file content"],
        expected_output_contains="Inserted file with key"
    )

    assert not await maybe_await(remote_file_store.exists(key))

    await run(
        args=["dolt-annex", "filestore", "migrate", "--from", setup.local_repo.name, "--to", setup.remote_repo.name],
    )

    assert await maybe_await(local_file_store.exists(key))
    assert await maybe_await(remote_file_store.exists(key))

    await run(
        args=["dolt-annex", "filestore", "migrate", "--from", setup.local_repo.name, "--to", setup.remote_repo.name, "--remove-if-exists"],
    )

    assert not await maybe_await(local_file_store.exists(key))
    
    async with remote_file_store.with_file_object(key) as file_obj:
        content = await file_obj.read()
        assert content == b"new file content"
