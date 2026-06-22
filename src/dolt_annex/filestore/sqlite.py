#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQLite is a filestore type that stores every file in
a SQL archive file (https://sqlite.org/sqlar.html).

This is a SQLite database with a specific schema like so:

CREATE TABLE sqlar(
  name TEXT PRIMARY KEY,  -- name of the file
  mode INT,               -- access permissions
  mtime INT,              -- last modification time
  sz INT,                 -- original file size
  data BLOB               -- compressed content
);

For our purposes, |mode| and |mtime| will always be NULL.
Data is stored uncompressed (sz == len(data)).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import pathlib
from typing import Tuple
from typing_extensions import override

import sqlite3

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, ReadableFileObject, ReadableStream
from dolt_annex.datatypes.async_utils import Result, await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import AsyncBytesIO, async_bytes_io
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, FileStoreModel

SQLAR_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE sqlar(
  name TEXT PRIMARY KEY,  -- name of the file
  mode INT,               -- access permissions
  mtime INT,              -- last modification time
  sz INT,                 -- original file size
  data BLOB               -- compressed content
);
"""

SQLAR_DB_FILENAME = "annex.sqlar"


class SQLite(FileStore):
    """
    This filestore uses SQLite Archive files (https://www.sqlite.org/sqlar.html) to store files.
    """

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey) -> Result[None]:
        async with data_source as in_fd:
            data = await in_fd.read()
        self.db.execute(
            "INSERT OR REPLACE INTO sqlar(name, mode, mtime, sz, data) VALUES (?, NULL, NULL, ?, ?)",
            (str(file_key), len(data), data),
        )
        self.db.commit()
        return Result.done()

    @override
    @await_or_enter
    async def get_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        row = self.db.execute(
            "SELECT data FROM sqlar WHERE name = ?", (str(file_key),)
        ).fetchone()
        if row is None:
            raise FileNotFoundError(f"File with key {file_key} not found in SQLite archive.")
        yield AsyncBytesIO(row[0])

    @override
    def exists(self, file_key: FileKey) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM sqlar WHERE name = ?", (str(file_key),)
        ).fetchone()
        return row is not None

    @override
    def stat(self, file_key: FileKey) -> FileInfo:
        row = self.db.execute(
            "SELECT sz FROM sqlar WHERE name = ?", (str(file_key),)
        ).fetchone()
        if row is None:
            raise FileNotFoundError(f"File with key {file_key} not found in SQLite archive.")
        return FileInfo(size=row[0])

    @override
    def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, AsyncBytesIO):
            raise TypeError("SQLite.fstat was passed a file object that did not originate from this filestore.")
        return FileInfo(size=len(file_obj.data))

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> Result[None]:
        self.db.execute(
            "INSERT OR REPLACE INTO sqlar(name, mode, mtime, sz, data) "
            "SELECT ?, mode, mtime, sz, data FROM sqlar WHERE name = ?",
            (str(new_key), str(old_key)),
        )
        self.db.commit()
        return Result.done()

    async def get_files(self, prefix: bytes = b"") -> AsyncGenerator[Tuple[FileKey, AwaitOrEnter[ReadableStream]]]:
        if prefix:
            prefix_str = str(prefix, encoding='utf-8')
            prefix_str = (prefix_str
                      .replace('\\', '\\\\')
                      .replace('%', '\\%')
                      .replace('_', '\\_')) + '%'
            query = "SELECT name, data FROM sqlar WHERE name LIKE ? ESCAPE '\\'"
            params = (prefix_str,)
        else:
            query = "SELECT name, data FROM sqlar"
            params = ()
        rows = self.db.execute(query, params).fetchall()
        for name, data in rows:
            yield FileKey.must_parse(name), async_bytes_io(data)

    @override
    def delete(self, key: FileKey) -> None:
        """
        Remove a file from a filestore.
        """
        self.db.execute("DELETE FROM sqlar WHERE name = ?", (str(key),))

class SQLiteModel(FileStoreModel):

    root: pathlib.Path

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[SQLite]:
        """Open a SQLite Archive, creating and initializing it if it does not exist."""
        self.root.mkdir(parents=True, exist_ok=True)
        db_path = self.root / SQLAR_DB_FILENAME
        db = sqlite3.connect(db_path)
        try:
            existing = db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sqlar'"
            ).fetchone()
            if existing is None:
                db.executescript(SQLAR_SCHEMA)
                db.commit()
            else:
                # Validate that the table has the expected sqlar columns.
                db.execute("SELECT name, mode, mtime, sz, data FROM sqlar LIMIT 1")
            yield SQLite(db=db)
        finally:
            db.close()
