#!/usr/bin/env python
# -*- coding: utf-8 -*-

from calendar import c
from contextlib import asynccontextmanager
import contextlib
import pathlib
import random
import tempfile
from tokenize import maybe
import pytest_asyncio
from typing_extensions import Generator, AsyncGenerator, override
import pytest

import asyncssh
import fs.memoryfs

from dolt_annex import test_util
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.common import SSHConnection
from dolt_annex.datatypes.file_io import Path, async_bytes_io
from dolt_annex.file_keys import Sha256E, MD5e, SHA1e
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
                file_key_format=Sha256E,
                alternate_key_formats=[SHA1e]
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
    yield ArchiveFSModel(num_workers=1, root=fs.memoryfs.MemoryFS(), secondary=MemoryFSModel())


def all_filestore_types() -> Generator[FileStoreModel]:
    yield from local_filestore_types()
    for fs in local_filestore_types():
        yield SftpWrappedFilestoreModel(remote_file_store_model=fs)
    yield SimpleSftpFilestoreModel()

def all_filestore_type_parameters():
    for fs in all_filestore_types():
        yield pytest.param(fs, id=fs.type_name())

@pytest_asyncio.fixture(params=all_filestore_type_parameters())
async def cas(request, test_config) -> AsyncGenerator[ContentAddressableStorage]:
    filestore_model: FileStoreModel = request.param
    with (
        tempfile.TemporaryDirectory() as temp_dir,
        contextlib.chdir(temp_dir)
    ):
        async with filestore_model.open(test_config) as filestore:
            yield ContentAddressableStorage(filestore, Sha256E, [SHA1e])

@pytest.mark.asyncio
async def test_file_stores(cas: ContentAddressableStorage):
    file_bytes = b"test"
    # This test uses SHA256 as the main key format, with SHA1 as an alternate,
    # and creates an MD5 file key alias explicitly.
    md5_key = MD5e.from_bytes(file_bytes)
    sha1_key = SHA1e.from_bytes(file_bytes)
    sha256_key = Sha256E.from_bytes(file_bytes)
    result = await cas.put_file_object(async_bytes_io(file_bytes), file_key=sha256_key)
    await result.wait_for_complete()
    # TODO: Test that putting the same key again short-circuits
    # TODO: Test having the cas generate both keys, check that the provided key is among the computed keys
    await cas.file_store.create_alias(sha256_key, md5_key)
    await maybe_await(cas.file_store.flush())
    for key in (sha256_key, md5_key, sha1_key):
        assert await maybe_await(cas.file_store.exists(key))
        file_info = await maybe_await(cas.file_store.stat(key))
        assert file_info.size == 4
        async with cas.file_store.with_file_object(key) as f:
            file_info = await maybe_await(cas.file_store.fstat(f))
            assert file_info.size == 4
            read_bytes = await f.read()
            assert read_bytes == b"test"

    # Check that exist for non-existent file returns false
    assert not await maybe_await(cas.file_store.exists(Sha256E.from_bytes(b"nonexistent")))


@pytest.mark.asyncio
async def test_unionfs(temp_dir: pathlib.Path, test_config: Config):
    child_filestores = [
        ArchiveFSModel(num_workers=1, root=fs.memoryfs.MemoryFS(), secondary=MemoryFSModel()),
        MemoryFSModel(),
        AnnexFSModel(root=temp_dir),
    ]
    async with UnionFSModel(children=child_filestores).open(test_config) as union_filestore:

        # Create a file in each child filestore
        for i, child in enumerate(union_filestore.children):
            file_bytes = f"file_in_child_{i}".encode()
            file_key = Sha256E.from_bytes(file_bytes)
            result = await maybe_await(child.put_file_bytes(file_bytes, file_key=file_key))
            await result.wait_for_complete()

        for i in range(len(union_filestore.children)):
            file_bytes = f"file_in_child_{i}".encode()
            file_key = Sha256E.from_bytes(file_bytes)
            assert await maybe_await(union_filestore.exists(file_key))
            async with union_filestore.with_file_object(file_key) as f:
                read_bytes = await f.read()
                assert read_bytes == file_bytes

        # Creating a child in the parent UnionFS creates the file in the first child filestore
        new_file_key = Sha256E.from_bytes(b"new_file")
        result = await maybe_await(union_filestore.put_file_bytes(b"new_file", new_file_key))
        await result.wait_for_complete()
        assert await maybe_await(union_filestore.children[0].exists(new_file_key))

        # Creating an alias in the UnionFS creates the alias in the child filestore where the original file exists

        for i, child in enumerate(union_filestore.children):
            file_bytes = f"file_in_child_{i}".encode()
            file_key = Sha256E.from_bytes(file_bytes)
            alias_key = MD5e.from_bytes(file_bytes)
            result = await maybe_await(union_filestore.create_alias(file_key, alias_key))
            await result.wait_for_complete()
            assert await maybe_await(child.exists(alias_key))

if __name__ == "__main__":
    pytest.main([__file__])