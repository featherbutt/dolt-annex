#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
import random
from typing_extensions import Callable, Generator, ContextManager, AsyncGenerator
import pytest

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.common import Connection
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.base import ContentAddressableStorage, FileStore, maybe_await
from dolt_annex.filestore.memory import MemoryFS
from dolt_annex.filestore.unionfs import UnionFS
from dolt_annex.filestore.sftp import SftpFileStore
from dolt_annex.server.ssh import server_context as async_server_context

type FileStoreFactory = Callable[[Path], ContextManager[FileStore]]



def filestore_types():
    @contextmanager
    def memory_fs_factory(tmp_path: Path) -> Generator[FileStore, None, None]:
        yield MemoryFS()
    yield pytest.param(memory_fs_factory, id="memory")

    @contextmanager
    def annex_fs_factory(tmp_path: Path) -> Generator[FileStore, None, None]:
        yield AnnexFS(root=tmp_path / "annex")
    yield pytest.param(annex_fs_factory, id="annex")

    @contextmanager
    def union_fs_factory(tmp_path: Path) -> Generator[FileStore, None, None]:
        yield UnionFS(children=[MemoryFS()])
    yield pytest.param(union_fs_factory, id="unionfs")

@pytest.fixture(params=[True, False])
def use_sftp(request) -> bool:
    return request.param

@asynccontextmanager
async def sftp_filestore_factory(tmp_path, remote_file_cas: ContentAddressableStorage) -> AsyncGenerator[ContentAddressableStorage]:
    if not use_sftp:
        yield remote_file_cas
        return

    host = "localhost"
    ssh_port = random.randint(21000, 22000)

    local_file_cas = ContentAddressableStorage(
        file_store = SftpFileStore(url=Connection(
            host=host,
            port=ssh_port,
            client_key=Path(__file__).parent.parent.parent.parent / "tests" / "test_client_keys" / "id_ed25519",
        )),
        file_key_format=Sha256e,
    )

    # setup server, then create server context, then setup client.
    async with (
            async_server_context(
            remote_file_cas,
            host,
            ssh_port,
            str(Path(__file__).parent.parent.parent.parent / "tests" / "test_client_keys" / "id_ed25519.pub"),
            str(Path(__file__).parent.parent.parent.parent / "tests" / "test_client_keys" / "id_ed25519")
        ),
        local_file_cas.file_store.open(base_config)
    ):
        yield local_file_cas

@pytest.fixture(params=filestore_types())
def cas(tmp_path: Path, request) -> Generator[ContentAddressableStorage]:
    factory: FileStoreFactory = request.param
    with factory(tmp_path) as fs:
        inner_cas = ContentAddressableStorage(fs, Sha256e)
        yield inner_cas

base_config = Config()

@pytest.mark.asyncio
async def test_file_stores(tmp_path, cas: ContentAddressableStorage, use_sftp: bool):
    if use_sftp:
        sftp_context = sftp_filestore_factory(tmp_path, cas)
    else:
        sftp_context = cas.open(base_config)
    async with sftp_context as cas:
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