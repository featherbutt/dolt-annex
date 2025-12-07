#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from contextlib import asynccontextmanager
from io import BytesIO
import pathlib
from typing import cast
from typing_extensions import override



from fs.base import FS as FileSystem

from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import ReadableFileObject
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileObject, FileStore

plyvel_imported = False
try:
    import plyvel
    plyvel_imported = True
except ImportError:
    pass  # plyvel is an optional dependency


class LevelDB(FileStore):

    root: pathlib.Path

    _db: plyvel.DB = None

    @override
    @asynccontextmanager
    async def open(self, config: Config):
        """Connect to a LevelDB database."""
        if not plyvel_imported:
            raise ImportError("plyvel is required for LevelDB filestore support. Please install dolt-annex with the 'leveldb' extra.")
        if self._db:
            yield
            return
        
        with plyvel.DB(self.root.as_posix(), create_if_missing=True) as self._db:
            yield

    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        self._db.put(bytes(file_key), await maybe_await(in_fd.read()))

    @override
    def get_file_object(self, file_key: FileKey) -> FileObject:
        file_bytes = self._db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return BytesIO(file_bytes)
    
    @override
    def stat(self, file_key: FileKey) -> FileInfo:
        file_bytes = self._db.get(bytes(file_key))
        if file_bytes is None:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return FileInfo(size=len(file_bytes))

    @override
    def fstat(self, file_obj: ReadableFileObject) -> FileInfo:
         b = cast(BytesIO, file_obj)
         return FileInfo(size=len(b.getvalue()))
    
    @override
    def exists(self, file_key: FileKey) -> bool:
        return self._db.get(bytes(file_key)) is not None