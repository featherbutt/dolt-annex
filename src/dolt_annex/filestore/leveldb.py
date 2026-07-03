#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import pathlib
from typing import Callable, Tuple
from typing_extensions import override

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, ReadableFileObject, ReadableStream, maybe_await
from dolt_annex.datatypes.async_utils import Result, await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import AsyncBytesIO, async_bytes_io
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, FileStoreModel

plyvel_imported = False
try:
    import plyvel
    plyvel_imported = True
except ImportError:
    pass  # plyvel is an optional dependency


class LevelDB(FileStore):

    db: plyvel.DB

    def __init__(self, *, db: plyvel.DB):
        self.db = db

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key_producer: Callable[[], FileKey]) -> FileKey:
        async with data_source as in_fd:
            value = await maybe_await(in_fd.read())
            file_key = file_key_producer()
            self.db.put(bytes(file_key), value, sync=True)
            return file_key

    @override
    @await_or_enter
    async def get_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        file_bytes = self.db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        yield AsyncBytesIO(file_bytes)

    @override
    def stat(self, file_key: FileKey) -> FileInfo:
        file_bytes = self.db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return FileInfo(size=len(file_bytes))

    @override
    def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, AsyncBytesIO):
            raise TypeError("LevelDB.fstat was passed a file object that did not originate from this filestore.")
        return FileInfo(size=len(file_obj.data))
    
    @override
    def exists(self, file_key: FileKey) -> bool:
        return self.db.get(bytes(file_key)) is not None

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> Result[None]:
        return await super().create_alias(old_key, new_key)

    async def get_files(self, prefix: bytes = b"") -> AsyncGenerator[Tuple[FileKey, AwaitOrEnter[ReadableStream]]]:
        for key, value in self.db.iterator(start=prefix):
            if not key.startswith(prefix):
                break
            yield FileKey.must_parse(key), async_bytes_io(value)

    @override
    def delete(self, key: FileKey) -> None:
        """
        Remove a file from a filestore.
        """
        self.db.delete(bytes(key), sync=True)
    

class LevelDBModel(FileStoreModel):

    root: pathlib.Path

    @override
    @asynccontextmanager
    async def open(self, config: Config):
        """Connect to a LevelDB database."""
        if not plyvel_imported:
            raise ImportError("plyvel is required for LevelDB filestore support. Please install dolt-annex with the 'leveldb' extra.")
        
        self.root.mkdir(parents=True, exist_ok=True)
        with plyvel.DB(self.root.as_posix(), create_if_missing=True) as db:
            yield LevelDB(db=db)