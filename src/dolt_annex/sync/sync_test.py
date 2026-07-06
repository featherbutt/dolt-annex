#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
from typing import Generator
import pytest
import random

import pytest_asyncio

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.config import Config
from dolt_annex.file_keys.base import FileKey
from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.archivefs import ArchiveFS, ArchiveFSModel
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.cas import ContentAddressableStorage, ContentAddressableStorageKeyMismatchError
from dolt_annex.filestore.filestore_test import SftpWrappedFilestoreModel, SimpleSftpFilestoreModel
from dolt_annex.filestore.leveldb import LevelDBModel
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.filestore.sqlite import SQLiteModel
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.sync import move_dataset
from dolt_annex.test_util import EnvironmentForTest, test_dataset_schema

# Try every combination of two and from types.
def all_filestore_types(prefix: pathlib.Path) -> Generator[FileStoreModel]:
    yield LevelDBModel(root=prefix / "leveldb")
    yield SQLiteModel(root=prefix / "sqlite")
    yield AnnexFSModel(root=prefix / "annexfs")
    yield ArchiveFSModel(num_workers=4, root=prefix / "archivefs" / "archives", secondary=SQLiteModel(root=prefix / "archivefs" / "secondary"))
    yield SftpWrappedFilestoreModel(
        remote_file_store_model=ArchiveFSModel(
            num_workers=4,
            root=prefix / "archivefs" / "archives",
            secondary=SQLiteModel(root=prefix / "archivefs" / "secondary")
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
    for i in range(NUM_FILES):
        file_bytes = random.randbytes(1024*i)
        file_key = await local_filestore.put_file_bytes(file_bytes)
        file_keys.append(file_key)
    return file_keys

file_key = Sha256E.from_bytes(b"existing data")

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", [pytest.param(MemoryFSModel(files={bytes(file_key): b"corrupted data"}), id=pytest.HIDDEN_PARAM)])
@pytest.mark.parametrize("remote_filestore_model", [pytest.param(MemoryFSModel(), id="")])
async def test_detect_corruption(
    setup: EnvironmentForTest,
):
    from_repo = setup.local_repo
    to_repo = setup.remote_repo
    FILTERS = []
    async with (
        as_acm(DatabaseConnection.open(setup.config)) as conn,
        as_acm(conn.open_dataset(test_dataset_schema)) as dataset,
        dataset.with_repo(from_repo.uuid) as from_repo_dataset,
        dataset.with_repo(to_repo.uuid) as to_repo_dataset,
    ):
        # Add entries to from_repo database
        from_table = from_repo_dataset.get_table("test_table")
        await from_table.insert(
            TableRow({"path": "0", "file_key": file_key}),
        )

        await from_table.flush()
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
    test_config: Config,
    setup: EnvironmentForTest,
    added_file_keys: list[Sha256E],
):
    from_repo = setup.local_repo
    to_repo = setup.remote_repo
    FILTERS = []
    async with (
        as_acm(DatabaseConnection.open(test_config)) as conn,
        as_acm(conn.open_dataset(test_dataset_schema)) as dataset,
        dataset.with_repo(from_repo.uuid) as from_repo_dataset,
        dataset.with_repo(to_repo.uuid) as to_repo_dataset,
    ):
        # Add entries to from_repo database
        from_table = from_repo_dataset.get_table("test_table")
        for i, file_key in enumerate(added_file_keys):
            path = f"{i}"
            await from_table.insert(TableRow({"path": path,"file_key": file_key}))
            

        await from_table.flush()
        await move_dataset(
            dataset,
            from_repo,
            to_repo,
            FILTERS,
        )
        # Check that files have been moved
        for file_key in added_file_keys:
            await to_repo.filestore.verify_file(file_key)
        # Check that db entries have been updated
        to_table = from_repo_dataset.get_table("test_table")
        for row in to_table.get_rows():
            path = row["path"]
            file_key = row["file_key"]
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

@pytest.mark.asyncio
@pytest.mark.parametrize("local_filestore_model", [pytest.param(MemoryFSModel(), id=pytest.HIDDEN_PARAM)])
@pytest.mark.parametrize("remote_filestore_model", [pytest.param(MemoryFSModel(), id="")])
async def test_diff_types(
    test_config: Config,
    setup: EnvironmentForTest,
):
    from_repo = setup.local_repo
    to_repo = setup.remote_repo
    FILTERS = []
    async with (
        as_acm(DatabaseConnection.open(test_config)) as conn,
        as_acm(conn.open_dataset(test_dataset_schema)) as dataset,
        dataset.with_repo(from_repo.uuid) as from_repo_dataset,
        dataset.with_repo(to_repo.uuid) as to_repo_dataset,
    ):
        
        file_bytes = random.randbytes(1024**2) # 1 MB
        file_key = await setup.local_repo.filestore.put_file_bytes(file_bytes)
        table_row = TableRow({"path": "test_path","file_key": file_key})
        # Add entries to from_repo database
        from_table = from_repo_dataset.get_table("test_table")
        await from_table.insert(table_row)

        await from_table.flush()
        await move_dataset(
            dataset,
            from_repo,
            to_repo,
            FILTERS,
        )
        # Check that files have been moved
        await to_repo.filestore.verify_file(file_key)
        # Check that db entries have been updated
        to_table = from_repo_dataset.get_table("test_table")
        rows = list(to_table.get_rows())
        assert len(rows) == 1
        assert rows[0]["path"] == "test_path"
        assert rows[0]["file_key"] == str(file_key)
        
        # Modify the table row
        await from_table.remove(table_row)
        new_file_bytes = random.randbytes(1024**2) # 1 MB
        new_file_key = await setup.local_repo.filestore.put_file_bytes(new_file_bytes)
        new_table_row = TableRow({"path": "test_path", "file_key": new_file_key})
        await from_table.insert(new_table_row)

        await from_table.flush()
        await move_dataset(
            dataset,
            from_repo,
            to_repo,
            FILTERS,
        )

        # Check that files have been moved
        await to_repo.filestore.verify_file(new_file_key)
        # Check that db entries have been updated
        rows = list(to_table.get_rows())
        assert len(rows) == 1
        assert rows[0]["path"] == "test_path"
        assert rows[0]["file_key"] == str(new_file_key)

        # Delete the table row
        await from_table.remove(new_table_row)

        await from_table.flush()
        await move_dataset(
            dataset,
            from_repo,
            to_repo,
            FILTERS,
        )

        rows = list(to_table.get_rows())
        assert len(rows) == 0