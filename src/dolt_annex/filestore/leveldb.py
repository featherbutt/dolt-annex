#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from io import BytesIO
import pathlib
from typing_extensions import cast, override

from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import FileObject, ReadableFileObject
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
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        self.db.put(bytes(file_key), await maybe_await(in_fd.read()))

    @override
    def get_file_object(self, file_key: FileKey) -> FileObject:
        file_bytes = self.db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return BytesIO(file_bytes)
    
    @override
    def stat(self, file_key: FileKey) -> FileInfo:
        file_bytes = self.db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return FileInfo(size=len(file_bytes))

    @override
    def fstat(self, file_obj: ReadableFileObject) -> FileInfo:
         b = cast(BytesIO, file_obj)
         return FileInfo(size=len(b.getvalue()))
    
    @override
    def exists(self, file_key: FileKey) -> bool:
        return self.db.get(bytes(file_key)) is not None

class LevelDBModel(FileStoreModel):

    root: pathlib.Path

    @override
    @asynccontextmanager
    async def open(self, config: Config):
        """Connect to a LevelDB database."""
        if not plyvel_imported:
            raise ImportError("plyvel is required for LevelDB filestore support. Please install dolt-annex with the 'leveldb' extra.")
        
        with plyvel.DB(self.root.as_posix(), create_if_missing=True) as db:
            yield LevelDB(db=db)