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

from plumbum import local
import pytest
import pytest_asyncio

from dolt_annex.data import data_dir
from dolt_annex.config.base import Config
from dolt_annex.config.filestore import FilestoreConfig
from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.file_keys import Sha256E
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.datatypes.file_stream import bytes_file_source
from dolt_annex.util.test_util import EnvironmentForTest

@pytest.fixture(params=[pytest.param({}, id="")])
def filestore_config(request) -> FilestoreConfig:
    return FilestoreConfig(**request.param)

@pytest.fixture
def test_config(filestore_config: FilestoreConfig) -> Config:
    return Config(
        filestore=filestore_config,
    )

@pytest.fixture
def local_uuid() -> UUID:
    return UUID("3fca31d9-f0dd-424e-b0e9-3cd4a26e9d68")

@pytest.fixture
def remote_uuid() -> UUID:
    return UUID("36b60d94-fbdf-476b-9479-f0abc61fa5ba")

@pytest.fixture
def local_filestore_model(local_uuid: UUID) -> FileStoreModel:
    return MemoryFSModel(uuid=local_uuid)

@pytest.fixture
def remote_filestore_model(remote_uuid: UUID) -> FileStoreModel:
    return MemoryFSModel(uuid=remote_uuid)

@pytest.fixture
def key_format() -> type[FileKey]:
    return Sha256E

@pytest.fixture
def alternate_key_formats() -> list[type[FileKey]]:
    return []

@pytest.fixture
def is_content_addressed() -> bool:
    return True

@pytest.fixture
def local_repo_model(
    local_uuid: UUID,
    local_filestore_model: FileStoreModel,
    key_format: type[FileKey],
    alternate_key_formats: list[type[FileKey]],
    is_content_addressed: bool,
) -> RepoModel:
    return RepoModel(
        name="__local__",
        uuid=local_uuid,
        alternate_key_formats=alternate_key_formats,
        filestore=local_filestore_model,
        content_addressed=is_content_addressed
    )

@pytest.fixture
def remote_repo_model(
    remote_uuid: UUID, 
    remote_filestore_model: FileStoreModel,
    key_format: type[FileKey],
    alternate_key_formats: list[type[FileKey]],
    is_content_addressed: bool,
) -> RepoModel:
    return RepoModel(
        name="test_remote",
        uuid=remote_uuid,
        alternate_key_formats=alternate_key_formats,
        filestore= remote_filestore_model,
        content_addressed=is_content_addressed
    )

@pytest.fixture
def temp_dir(tmp_path):
    tmp_path = pathlib.Path(tmp_path)
    with contextlib.chdir(tmp_path):
        yield tmp_path

@pytest.fixture(scope="session")
def init_session_dolt(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    template_dir = tmp_path_factory.mktemp("init_dolt_template") / "dolt"
    template_dir.mkdir()
    dolt = local.cmd.dolt.with_cwd(template_dir)
    shutil.copytree(data_dir / "dolt_base" / ".dolt", template_dir / ".dolt")
    dolt("checkout", "-b", "test_dataset")
    dolt("sql", "-q", "CREATE TABLE test_table(path varchar(100) primary key, file_key varchar(100));")
    dolt("add", ".")
    dolt("commit", "-m", "Initial commit")
    return template_dir / ".dolt"

@pytest.fixture
def init_dolt(temp_dir: pathlib.Path, init_session_dolt: pathlib.Path):
    dolt_dir = pathlib.Path(temp_dir / "dolt")
    dolt_dir.mkdir()
    shutil.copytree(init_session_dolt, dolt_dir / ".dolt")
    yield local.cmd.dolt.with_cwd(dolt_dir)

@contextlib.asynccontextmanager
async def create_test_filestore(config: Config, repo_model: RepoModel, files: Iterable[bytes]) -> AsyncGenerator[ContentAddressableStorage]:
    async with repo_model.filestore.open(config) as filestore:
        cas = ContentAddressableStorage(config.filestore, filestore, repo_model.alternate_key_formats, content_addressed=repo_model.content_addressed)
        for file_content in files:
            await cas.put_file(bytes_file_source(file_content, Sha256E))
        yield cas

@pytest.fixture(autouse=True)
def loadable_context():
    with Loadable.context():
        yield

@pytest_asyncio.fixture 
async def local_filestore(temp_dir: pathlib.Path, local_repo_model: RepoModel, test_config: Config) -> AsyncGenerator[ContentAddressableStorage]:
    async with create_test_filestore(test_config, local_repo_model, []) as local_filestore:
        yield local_filestore

@pytest_asyncio.fixture 
async def remote_filestore(temp_dir: pathlib.Path, remote_repo_model: RepoModel, test_config: Config) -> AsyncGenerator[ContentAddressableStorage]:
    async with create_test_filestore(test_config, remote_repo_model, []) as remote_filestore:
        yield remote_filestore

@pytest.fixture
def local_repo(local_repo_model: RepoModel, local_filestore: ContentAddressableStorage) -> Repo:
    return Repo(
        name=local_repo_model.name,
        uuid=local_repo_model.uuid,
        filestore=local_filestore,
        alternate_key_formats=local_repo_model.alternate_key_formats,
    )

@pytest.fixture
def remote_repo(remote_repo_model: RepoModel, remote_filestore: ContentAddressableStorage) -> Repo:
    return Repo(
        name=remote_repo_model.name,
        uuid=remote_repo_model.uuid,
        filestore=remote_filestore,
        alternate_key_formats=remote_repo_model.alternate_key_formats,
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
    test_config: Config
    ) -> AsyncGenerator[EnvironmentForTest]:
        with (temp_dir / "config.json").open("w") as f:
            f.write(test_config.model_dump_json())

        yield EnvironmentForTest(
            local_repo=local_repo,
            remote_repo=remote_repo,
            config=test_config
        )