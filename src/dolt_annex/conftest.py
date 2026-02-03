#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module contains pytest fixtures for setting up tests.
"""

import pathlib
import shutil
import contextlib
from typing import AsyncGenerator, Iterable
from uuid import UUID

from plumbum import local # type: ignore
import pytest
import pytest_asyncio

from dolt_annex.data import data_dir
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.test_util import test_config, EnvironmentForTest

@pytest.fixture
def base_config() -> Config:
    return Config()

@pytest.fixture
def local_uuid() -> UUID:
    return UUID("3fca31d9-f0dd-424e-b0e9-3cd4a26e9d68")

@pytest.fixture
def remote_uuid() -> UUID:
    return UUID("36b60d94-fbdf-476b-9479-f0abc61fa5ba")

@pytest.fixture
def local_filestore_model() -> FileStoreModel:
    return MemoryFSModel()

@pytest.fixture
def remote_filestore_model() -> FileStoreModel:
    return MemoryFSModel()

@pytest.fixture
def local_repo_model(local_uuid: UUID, local_filestore_model: FileStoreModel) -> RepoModel:
    return RepoModel(
        name="__local__",
        uuid=local_uuid,
        key_format=Sha256E,
        filestore= local_filestore_model
    )

@pytest.fixture
def remote_repo_model(remote_uuid: UUID, remote_filestore_model: FileStoreModel) -> RepoModel:
    return RepoModel(
        name="test_remote",
        uuid=remote_uuid,
        key_format=Sha256E,
        filestore= remote_filestore_model
    )

@pytest.fixture
def temp_dir(tmp_path: pathlib.Path):
    with contextlib.chdir(tmp_path):
        yield tmp_path

@pytest.fixture
def dolt(temp_dir: pathlib.Path):
    dolt_dir = pathlib.Path(temp_dir / "dolt")
    dolt_dir.mkdir()
    dolt = local.cmd.dolt.with_cwd(dolt_dir)
    shutil.copytree(data_dir / "dolt_base" / ".dolt", dolt_dir / ".dolt")
    yield dolt

@pytest.fixture
def init_dolt(dolt):
    dolt("checkout", "-b", "test_dataset")
    dolt("sql", "-q", "CREATE TABLE test_table(path varchar(100) primary key, annex_key varchar(100));")
    dolt("add", ".")
    dolt("commit", "-m", "Initial commit")
    yield dolt

@contextlib.asynccontextmanager
async def create_test_filestore(filestore_model: FileStoreModel, files: Iterable[bytes]) -> AsyncGenerator[ContentAddressableStorage]:
    async with filestore_model.open(test_config) as filestore:
        cas = ContentAddressableStorage(filestore, Sha256E)
        for file_content in files:
            await cas.put_file_bytes(file_content)
        yield cas

@pytest.fixture(autouse=True)
def loadable_context():
    with Loadable.context():
        yield

@pytest_asyncio.fixture 
async def local_filestore(temp_dir: pathlib.Path, local_uuid: UUID, local_filestore_model: FileStoreModel) -> AsyncGenerator[ContentAddressableStorage]:
    async with create_test_filestore(local_filestore_model, []) as local_filestore:
        yield local_filestore

@pytest_asyncio.fixture 
async def remote_filestore(temp_dir: pathlib.Path, remote_uuid: UUID, remote_filestore_model: FileStoreModel) -> AsyncGenerator[ContentAddressableStorage]:
    async with create_test_filestore(remote_filestore_model, []) as remote_filestore:
        yield remote_filestore

@pytest.fixture
def local_repo(local_uuid: UUID, local_filestore: ContentAddressableStorage) -> Repo:
    return Repo(
        name="__local__",
        uuid=local_uuid,
        filestore=local_filestore.file_store,
        key_format=Sha256E
    )

@pytest.fixture
def remote_repo(remote_uuid: UUID, remote_filestore: ContentAddressableStorage) -> Repo:
    return Repo(
        name="test_remote",
        uuid=remote_uuid,
        filestore=remote_filestore.file_store,
        key_format=Sha256E
    )

@pytest_asyncio.fixture 
async def setup(
    temp_dir: pathlib.Path,
    init_dolt,
    local_filestore: ContentAddressableStorage,
    remote_filestore: ContentAddressableStorage,
    local_repo_model: RepoModel,
    remote_repo_model: RepoModel,
    local_repo: Repo,
    remote_repo: Repo,
    ) -> AsyncGenerator[EnvironmentForTest]:
        with (temp_dir / "config.json").open("w") as f:
            f.write(test_config.model_dump_json())

        yield EnvironmentForTest(
            local_file_store=local_filestore,
            local_repo=local_repo,
            remote_file_store=remote_filestore,
            remote_repo=remote_repo,
        )