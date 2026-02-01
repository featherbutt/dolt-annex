#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
from typing import Generator
import uuid
from plumbum import local
import pytest
import random

import pytest_asyncio

from dolt_annex.conftest import local_repo
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.file_keys.base import FileKey
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.archivefs import ArchiveFS, ArchiveFSModel
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.cas import ContentAddressableStorage, ContentAddressableStorageKeyMismatchError
from dolt_annex.filestore.filestore_test import SftpWrappedFilestoreModel, SimpleSftpFilestoreModel
from dolt_annex.filestore.leveldb import LevelDBModel
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.table import Dataset
from dolt_annex.sync import move_dataset
from dolt_annex.test_util import EnvironmentForTest, test_config, test_dataset_schema

# Try every combination of two and from types.
def all_filestore_types(prefix: pathlib.Path) -> Generator[FileStoreModel]:
    yield LevelDBModel(root=prefix / "leveldb")
    yield AnnexFSModel(root=prefix / "annexfs")
    yield ArchiveFSModel(num_workers=4, root=prefix / "archivefs" / "archives", secondary=LevelDBModel(root=prefix / "archivefs" / "secondary"))
    yield SftpWrappedFilestoreModel(
        remote_file_store_model=ArchiveFSModel(
            num_workers=4,
            root=prefix / "archivefs" / "archives",
            secondary=LevelDBModel(root=prefix / "archivefs" / "secondary")
        )
    )
    yield SimpleSftpFilestoreModel()

def all_filestore_type_parameters(prefix: pathlib.Path):
    for fs in all_filestore_types(prefix):
        yield pytest.param(fs, id=fs.type_name())

@pytest_asyncio.fixture
async def added_file_keys(local_filestore: ContentAddressableStorage) -> list[FileKey]:
    # Create random files to move in parallel
    NUM_FILES = 5
    file_keys: list[FileKey] = []
    for _ in range(NUM_FILES):
        file_bytes = random.randbytes(1024**2) # 1 MB
        file_key_result = await local_filestore.put_file_bytes(file_bytes)
        file_key = await file_key_result.wait_for_complete()
        file_keys.append(file_key)
    return file_keys

file_key = Sha256e.from_bytes(b"existing data")

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", [pytest.param(MemoryFSModel(files={bytes(file_key): b"corrupted data"}), id="")])
@pytest.mark.parametrize("remote_filestore_model", [pytest.param(MemoryFSModel(), id="")])
async def test_detect_corruption(
    setup: EnvironmentForTest,
):
    from_repo = setup.local_repo
    to_repo = setup.remote_repo
    BATCH_SIZE = 1000 # Arbitrary batch size for this command
    FILTERS = [] # Allow for any setup delays
    async with Dataset.connect(test_config, BATCH_SIZE, test_dataset_schema) as dataset:
        # TODO: handle initializing branches automatically
        dataset.dolt.initialize_dataset_source(test_dataset_schema, from_repo.uuid)
        dataset.dolt.initialize_dataset_source(test_dataset_schema, to_repo.uuid)
        # Add entries to from_repo database
        table = dataset.get_table("test_table")
        await table.insert_file_source(
            TableRow(("0",)),
            file_key,
            from_repo.uuid,
        )

        await table.flush()
        with pytest.RaisesGroup(ContentAddressableStorageKeyMismatchError, flatten_subgroups=True, allow_unwrapped=True):
            await move_dataset(
                dataset,
                from_repo,
                to_repo,
                FILTERS,
            )

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", all_filestore_type_parameters(pathlib.Path("from")))
@pytest.mark.parametrize("remote_filestore_model", all_filestore_type_parameters(pathlib.Path("to")))
async def test_async_move(
    setup: EnvironmentForTest,
    added_file_keys: list[Sha256e],
):
    from_repo = setup.local_repo
    to_repo = setup.remote_repo
    BATCH_SIZE = 1000 # Arbitrary batch size for this command
    FILTERS = [] # Allow for any setup delays
    async with Dataset.connect(test_config, BATCH_SIZE, test_dataset_schema) as dataset:
        # TODO: handle initializing branches automatically
        dataset.dolt.initialize_dataset_source(test_dataset_schema, from_repo.uuid)
        dataset.dolt.initialize_dataset_source(test_dataset_schema, to_repo.uuid)
        # Add entries to from_repo database
        table = dataset.get_table("test_table")
        for i, file_key in enumerate(added_file_keys):
            path = f"{i}"
            await table.insert_file_source(
                TableRow((path,)),
                file_key,
                from_repo.uuid,
            )

        await table.flush()
        await move_dataset(
            dataset,
            from_repo,
            to_repo,
            FILTERS,
        )
        # Check that files have been moved
        for file_key in added_file_keys:
            assert await maybe_await(to_repo.filestore.exists(file_key))
            assert await to_repo.filestore.get_file_bytes(file_key) == await from_repo.filestore.get_file_bytes(file_key)
        # Check that db entries have been updated
        for file_key, path in table.get_rows(to_repo.uuid):
            assert bytes(added_file_keys[int(path)]) == bytes(file_key, encoding='utf-8')
        # If the destination filestore is ArchiveFS, ensure that files are in multiple archives
        if isinstance(to_repo.filestore, ArchiveFS):
            archive_ids = set()
            for file_key in added_file_keys:
                file_location = await to_repo.filestore.secondary.get_file_bytes(file_key)
                print(file_location)
                name, offset, size = file_location.split(b":")
                archive_ids.add(name)
            # assert len(archive_ids) > 1
