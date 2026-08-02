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
import logging
import pathlib
from typing import Awaitable, Self, Tuple
from typing_extensions import override

import sqlite3

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, ReadableFileObject, ReadableStream
from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import AsyncBytesIO, async_bytes_io
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, FileStoreModel

logger = logging.getLogger(__name__)

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

# Setting MAX_BATCH_SIZE to 0 disables batched writes.
# This is safest and usually the correct behavior,
# because SQLite writes are rarely the bottleneck.
# This may be added as a config flag later, but right now 
# it suffices to change this value when you really need batched writes.
# TODO: If a process terminates during a batched write, are we
# guarenteed to be in a consistent state? Do callers to put_file_object
# assume that the file is persisted to disk after calling?

# MAX_BATCH_SIZE = 2 * 1024 * 1024  # 2 MB
MAX_BATCH_SIZE = 0

class SQLite(FileStore):
    """
    This filestore uses SQLite Archive files (https://www.sqlite.org/sqlar.html) to store files.
    """

    db: sqlite3.Connection
    pending: dict[FileKey, bytes]
    pending_size: int

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self.pending = {}
        self.pending_size = 0

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key_producer: Awaitable[FileKey], overwrite_existing: bool = False) -> FileKey:
        async with data_source as in_fd:
            data = await in_fd.read()
        file_key = await file_key_producer
        # TODO: We could have two different pending lists for rows that should overwrite.
        if overwrite_existing:
            self.db.execute(
                "INSERT OR REPLACE INTO sqlar(name, mode, mtime, sz, data) VALUES (?, NULL, NULL, ?, ?)",
                (str(file_key), len(data), data)
            )
        else:
            self.pending[file_key] = data
            self.pending_size += len(data)
            if self.pending_size >= MAX_BATCH_SIZE:
                logger.info("Flushing SQLite filestore with %s added records ending in %s", len(self.pending), str(file_key))
                await self.flush()
                
        return file_key
    
    async def flush(self):
        self.db.executemany(
            "INSERT OR IGNORE INTO sqlar(name, mode, mtime, sz, data) VALUES (?, NULL, NULL, ?, ?)",
            ((str(file_key), len(data), data) for file_key, data in self.pending.items())
        )
        self.db.commit()
        self.pending.clear()
        self.pending_size = 0

        

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
    async def exists(self, file_key: FileKey) -> bool:
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
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        if old_key in self.pending:
            await self.put_file_bytes(self.pending[old_key], new_key)
        else:
            self.db.execute(
                "INSERT OR REPLACE INTO sqlar(name, mode, mtime, sz, data) "
                "SELECT ?, mode, mtime, sz, data FROM sqlar WHERE name = ?",
                (str(new_key), str(old_key)),
            )
            self.db.commit()

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

    @classmethod
    @asynccontextmanager
    async def new(cls: type[Self], root: pathlib.Path) -> AsyncGenerator[Self, None]:
        """Open a SQLite Archive, creating and initializing it if it does not exist."""
        root.mkdir(parents=True, exist_ok=True)
        db_path = root / SQLAR_DB_FILENAME
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
            sqliteFS = cls(db=db)
            yield sqliteFS
            await sqliteFS.flush()
        finally:
            db.close()
        

class SQLiteModel(FileStoreModel):

    root: pathlib.Path

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[SQLite]:
        async with SQLite.new(self.root) as sqlite:
            yield sqlite
