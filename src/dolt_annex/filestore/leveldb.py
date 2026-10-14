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
from typing_extensions import override

from dolt_annex.constants import PROGNAME
from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream, maybe_await
from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import AsyncBytesIO, async_bytes_io
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_stream import KeyedFileStream, KnownKeyFileStream, known_key_source_nocheck

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
    async def put_file_object(self, data_source: KeyedFileStream, overwrite_existing: bool = False) -> FileStore.PutFileResult:
        async with data_source as in_fd:
            value = await maybe_await(in_fd.read())
        file_key = await data_source.file_key()
        if overwrite_existing or self.db.get(bytes(file_key)) is None:
            self.db.put(bytes(file_key), value, sync=True)
        return FileStore.PutFileResult(file_key=file_key, uuids=[self.uuid] if self.uuid else [])

    @override
    @await_or_enter
    async def with_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        file_bytes = self.db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        yield AsyncBytesIO(file_bytes)

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        file_bytes = self.db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return FileInfo(size=len(file_bytes))

    @override
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, AsyncBytesIO):
            raise TypeError("LevelDB.fstat was passed a file object that did not originate from this filestore.")
        return FileInfo(size=len(file_obj.data))
    
    @override
    async def exists(self, file_key: FileKey) -> bool:
        return self.db.get(bytes(file_key)) is not None

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey):
        return await super().create_alias(old_key, new_key)

    async def get_files(self, prefix: bytes = b"") -> AsyncGenerator[KnownKeyFileStream]:
        for key, value in self.db.iterator(start=prefix):
            if not key.startswith(prefix):
                break
            yield known_key_source_nocheck(async_bytes_io(value), FileKey.must_parse(key))

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
            raise ImportError(f"plyvel is required for LevelDB filestore support. Please install {PROGNAME} with the 'leveldb' extra.")
        
        self.root.mkdir(parents=True, exist_ok=True)
        with plyvel.DB(self.root.as_posix(), create_if_missing=True) as db:
            yield LevelDB(db=db)