#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from contextlib import asynccontextmanager, contextmanager
from io import BytesIO
from pathlib import Path
from typing import cast
import plyvel
from typing_extensions import override

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import ReadableFileObject
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileObject, FileStore

class LevelDB(FileStore):

    root: Path

    _db: plyvel.DB = None

    @override
    @asynccontextmanager
    async def open(self, config: Config):
        """Connect to a LevelDB database."""

        with plyvel.DB(self.root.as_posix(), create_if_missing=True) as self._db:
            yield

    @override
    def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        self._db.put(bytes(file_key), in_fd.read())

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
    def fstat(self, file_obj: FileObject) -> FileInfo:
         b = cast(BytesIO, file_obj)
         return FileInfo(size=len(b.getvalue()))
    
    @override
    def exists(self, file_key: FileKey) -> bool:
        return self._db.get(bytes(file_key)) is not None