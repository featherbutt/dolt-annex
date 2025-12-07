#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager, contextmanager
import contextlib
from pathlib import Path
import pathlib
import random
import tempfile
from typing import override
from typing_extensions import Callable, Generator, ContextManager, AsyncGenerator
import pytest

import fs.memoryfs

from dolt_annex.datatypes import remote
from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.common import Connection
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.base import FileStore
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.leveldb import LevelDB
from dolt_annex.filestore.memory import MemoryFS
from dolt_annex.filestore.unionfs import UnionFS
from dolt_annex.filestore.sftp import SftpFileStore
from dolt_annex.server.ssh import server_context as async_server_context

class TestSftpFileStore(SftpFileStore):
    """
    A wrapper around SftpFileStore that creates a server on localhost, for testing.
    """

    remote_file_store: FileStore

    @classmethod
    def make(cls, remote_file_store: FileStore):
        client_key=Path(__file__).parent.parent.parent.parent / "tests" / "test_client_keys" / "id_ed25519"
        port=random.randint(21000, 22000)
        connection = Connection(
            host="localhost",
            port=port,
            client_key=client_key
        )
        return cls(remote_file_store=remote_file_store, url=connection)

    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"SftpFileStore({self.remote_file_store.type_name()})"
    
    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[None]:
        remote_file_cas = ContentAddressableStorage(
            file_store=self.remote_file_store,
            file_key_format=Sha256e,
        )
        # setup server, then create server context, then setup client.
        async with (
            remote_file_cas.open(config),
            async_server_context(
                remote_file_cas,
                self.url.host,
                self.url.port,
                str(Path(__file__).parent.parent.parent.parent / "tests" / "test_client_keys" / "id_ed25519.pub"),
                str(Path(__file__).parent.parent.parent.parent / "tests" / "test_client_keys" / "id_ed25519")
            ),
            super().open(base_config)
        ):
            yield



def local_filestore_types():
    yield MemoryFS()
    yield LevelDB(root=pathlib.Path("leveldb"))
    yield AnnexFS(root=pathlib.Path("annex"), _file_system=fs.memoryfs.MemoryFS())
    yield UnionFS(children=[MemoryFS()])

def all_filestore_types():
    yield from local_filestore_types()
    for fs in local_filestore_types():
        yield TestSftpFileStore.make(fs)

def all_filestore_type_parameters():
    for fs in all_filestore_types():
        yield pytest.param(fs, id=fs.type_name())

@pytest.fixture(params=all_filestore_type_parameters())
def cas(request) -> Generator[ContentAddressableStorage]:
    filestore: FileStore = request.param
    yield ContentAddressableStorage(filestore, Sha256e)

base_config = Config()

@pytest.mark.asyncio
async def test_file_stores(cas: ContentAddressableStorage):
    with (
        tempfile.TemporaryDirectory() as temp_dir,
        contextlib.chdir(temp_dir)
    ):
        async with cas.open(base_config) as cas:
            test_key = await cas.put_file_bytes(b"test")
            assert await maybe_await(cas.file_store.exists(test_key))
            file_info = await maybe_await(cas.file_store.stat(test_key))
            assert file_info.size == 4
            async with cas.file_store.with_file_object(test_key) as f:
                file_info = await maybe_await(cas.file_store.fstat(f))
                assert file_info.size == 4
                read_bytes = await maybe_await(f.read())
                assert read_bytes == b"test"

if __name__ == "__main__":
    pytest.main([__file__])