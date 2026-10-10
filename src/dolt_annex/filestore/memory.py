#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MemoryFS is an in-memory filestore useful for testing. It does not persist files
across restarts.
"""

from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Awaitable, Tuple
from typing_extensions import override

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, ReadableFileObject, ReadableStream
from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import AsyncBytesIO, async_bytes_io
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_io import Path
from dolt_annex.file_keys.base import FileKeyPrefix
from dolt_annex.datatypes.file_stream import KeyedFileStream, KnownKeyFileStream, known_key_source, known_key_source_nocheck

from .base import FileInfo, FileStore, FileStoreModel

@dataclass(kw_only=True)
class MemoryFS(FileStore):

    files: dict[bytes, bytes] = field(default_factory=dict)

    @override
    async def put_file_object(self, data_source: KeyedFileStream, overwrite_existing: bool = False) -> FileStore.PutFileResult:
        """Copy a file-like object into the annex."""
        async with data_source as in_fd:
            value = await in_fd.read()
        file_key = await data_source.file_key()
        if overwrite_existing or bytes(file_key) not in self.files:
            self.files[bytes(file_key)] = value
        return FileStore.PutFileResult(file_key=file_key, uuids=[self.uuid] if self.uuid else [])

    @override
    async def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> FileStore.PutFileResult:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        self.files[bytes(file_key)] = file_bytes
        return FileStore.PutFileResult(file_key=file_key, uuids=[self.uuid] if self.uuid else [])

    @override
    @await_or_enter
    async def with_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        if bytes(file_key) not in self.files:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        yield AsyncBytesIO(self.files[bytes(file_key)])

    @override
    async def get_files(self, prefix: FileKeyPrefix = b"") -> AsyncGenerator[KnownKeyFileStream]:
        for file_key, value in self.files.items():
            if file_key.startswith(prefix):
                yield known_key_source_nocheck(async_bytes_io(value), FileKey.must_parse(file_key))
        
    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        return FileInfo(size=len(self.files[bytes(file_key)]))

    @override
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, AsyncBytesIO):
            raise TypeError("MemoryFS.fstat was passed a file object that did not originate from this filestore.")
        return FileInfo(size=len(file_obj.data))

    @override
    async def exists(self, file_key: FileKey) -> bool:
        return bytes(file_key) in self.files

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        self.files[bytes(new_key)] = self.files[bytes(old_key)]
    
    @override
    def delete(self, key: FileKey) -> None:
        del self.files[bytes(key)]

class MemoryFSModel(FileStoreModel):

    # Whether the filestore should persist files after being closed.
    persistent: bool = True
    files: dict[bytes, bytes] = {}

    def create(self, config: Config) -> MemoryFS:
        return MemoryFS(uuid=self.uuid, files=self.files if self.persistent else self.files.copy())