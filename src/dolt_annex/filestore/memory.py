#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MemoryFS is an in-memory filestore useful for testing. It does not persist files
across restarts.
"""

from collections.abc import AsyncGenerator
from typing import Callable, Tuple
from typing_extensions import override

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, ReadableFileObject, ReadableStream
from dolt_annex.datatypes.async_utils import Result, await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import AsyncBytesIO, async_bytes_io
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_io import Path
from dolt_annex.file_keys.base import FileKeyPrefix

from .base import FileInfo, FileStore, FileStoreModel

class MemoryFS(FileStore):

    files: dict[bytes, bytes]

    def __init__(self, files: dict[bytes, bytes] | None = None) -> None:
        super().__init__()
        if files is None:
            self.files = {}
        else:
            self.files = files

    @override
    async def put_file(self, file_path: Path, file_key: FileKey):
        """Move an on-disk file to the annex."""
        async with file_path.open() as f:
            self.files[bytes(file_key)] = await f.read()
             
    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key_producer: Callable[[], FileKey]) -> FileKey:
        """Copy a file-like object into the annex."""
        async with data_source as in_fd:
            value = await in_fd.read()
            file_key = file_key_producer()
            self.files[bytes(file_key)] = value
            return file_key

    @override
    async def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> None:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        self.files[bytes(file_key)] = file_bytes

    @override
    @await_or_enter
    async def get_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        if bytes(file_key) not in self.files:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        yield AsyncBytesIO(self.files[bytes(file_key)])

    @override
    async def get_files(self, prefix: FileKeyPrefix = b"") -> AsyncGenerator[Tuple[FileKey, AwaitOrEnter[ReadableStream]]]:
        for file_key, value in self.files.items():
            if file_key.startswith(prefix):
                yield FileKey.must_parse(file_key), async_bytes_io(value)
        
    @override
    def stat(self, file_key: FileKey) -> FileInfo:
        return FileInfo(size=len(self.files[bytes(file_key)]))

    @override
    def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, AsyncBytesIO):
            raise TypeError("MemoryFS.fstat was passed a file object that did not originate from this filestore.")
        return FileInfo(size=len(file_obj.data))

    @override
    def exists(self, file_key: FileKey) -> bool:
        return bytes(file_key) in self.files

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        self.files[bytes(new_key)] = self.files[bytes(old_key)]
    
    @override
    def delete(self, key: FileKey) -> None:
        del self.files[bytes(key)]

class MemoryFSModel(FileStoreModel):

    files: dict[bytes, bytes] = {}

    def create(self, config: Config) -> MemoryFS:
        return MemoryFS(files=self.files)