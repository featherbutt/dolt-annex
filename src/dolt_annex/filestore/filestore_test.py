#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager
import contextlib
import pathlib
import random
import tempfile
import pytest_asyncio
from typing_extensions import Generator, AsyncGenerator, override
import pytest

import asyncssh
import fs.memoryfs

from dolt_annex import test_util
from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.common import SSHConnection
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.filestore.archivefs import ArchiveFSModel
from dolt_annex.filestore.base import FileStore, FileStoreModel
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.leveldb import LevelDBModel
from dolt_annex.filestore.memory import MemoryFSModel
from dolt_annex.filestore.sftp import SftpFileStore
from dolt_annex.filestore.unionfs import UnionFSModel
from dolt_annex.server.ssh import server_context as async_server_context

class SimpleSftpFilestore(SftpFileStore):
    """
    A wrapper around SftpFileStore that creates a plain SFTP server.
    """

class SftpWrappedFileStore(SftpFileStore):
    """
    A wrapper around SftpFileStore that creates a server on localhost, for testing.
    """

    remote_file_store: FileStore

class SimpleSftpFilestoreModel(FileStoreModel):

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[SftpFileStore]:
        port=random.randint(21000, 22000)
        connection = SSHConnection(
            hostname="localhost",
            port=port,
            client_key=test_util.private_key_path
        )
        # setup server, then create server context, then setup client.
        server = await asyncssh.listen(connection.hostname, connection.port, server_host_keys=[str(test_util.private_key_path)],
                          authorized_client_keys=str(test_util.public_key_path),
                          sftp_factory=True)
        try:
            async with SimpleSftpFilestore.open(connection, config) as filestore:
                yield filestore
        finally:
            server.close()
            await server.wait_closed()

            

class SftpWrappedFilestoreModel(FileStoreModel):

    remote_file_store_model: FileStoreModel

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[SftpFileStore]:
        port=random.randint(21000, 22000)
        connection = SSHConnection(
            hostname="localhost",
            port=port,
            client_key=test_util.private_key_path
        )
        async with self.remote_file_store_model.open(config) as remote_file_store:
            remote_file_cas = ContentAddressableStorage(
                file_store=remote_file_store,
                file_key_format=Sha256e,
            )
            # setup server, then create server context, then setup client.
            async with (
                async_server_context(
                    remote_file_cas,
                    connection.hostname,
                    connection.port,
                    str(test_util.public_key_path),
                    str(test_util.private_key_path)
                ),
                SftpWrappedFileStore.open(connection, config) as filestore
            ):
                filestore.remote_file_store = remote_file_store
                yield filestore

    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"SftpFileStore({self.remote_file_store_model.type_name()})"




def local_filestore_types():
    yield MemoryFSModel()
    yield LevelDBModel(root=pathlib.Path("leveldb"))
    yield AnnexFSModel(root=fs.memoryfs.MemoryFS())
    yield UnionFSModel(children=[MemoryFSModel()])

def all_filestore_types() -> Generator[FileStoreModel]:
    yield from local_filestore_types()
    for fs in local_filestore_types():
        yield SftpWrappedFilestoreModel(remote_file_store_model=fs)
    yield SimpleSftpFilestoreModel()

def all_filestore_type_parameters():
    for fs in all_filestore_types():
        yield pytest.param(fs, id=fs.type_name())

@pytest.fixture
def base_config() -> Config:
    return Config()

@pytest_asyncio.fixture(params=all_filestore_type_parameters())
async def cas(request, base_config) -> AsyncGenerator[ContentAddressableStorage]:
    filestore_model: FileStoreModel = request.param
    with (
        tempfile.TemporaryDirectory() as temp_dir,
        contextlib.chdir(temp_dir)
    ):
        async with filestore_model.open(base_config) as filestore:
            yield ContentAddressableStorage(filestore, Sha256e)

@pytest.mark.asyncio
async def test_file_stores(cas: ContentAddressableStorage):

    test_key = await cas.put_file_bytes(b"test")
    assert await maybe_await(cas.file_store.exists(test_key))
    file_info = await maybe_await(cas.file_store.stat(test_key))
    assert file_info.size == 4
    async with cas.file_store.with_file_object(test_key) as f:
        file_info = await maybe_await(cas.file_store.fstat(f))
        assert file_info.size == 4
        read_bytes = await maybe_await(f.read())
        assert read_bytes == b"test"

    # Check that exist for non-existent file returns false
    assert not await maybe_await(cas.file_store.exists(Sha256e.from_bytes(b"nonexistent")))

if __name__ == "__main__":
    pytest.main([__file__])