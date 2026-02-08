#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
import pathlib
from typing_extensions import override

from fs.base import FS as FileSystem
import sqlite3

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import Path

from .base import FileStore, FileStoreModel

SQLAR_SCHEMA = """
CREATE TABLE sqlar(
  name TEXT PRIMARY KEY,  -- name of the file
  mode INT,               -- access permissions
  mtime INT,              -- last modification time
  sz INT,                 -- original file size
  data BLOB               -- compressed content
);
"""

class SQLite(FileStore):
    """
    This filestore uses SQLite Archive files (https://www.sqlite.org/sqlar.html) to store files.
    """

    db: sqlite3.Connection

    def __init__(self, file_system: FileSystem, file_path: Path):
        self.file_system = file_system
        self.file_path = file_path

    # TODO: Implmement necessary methods for FileStore

class SQLiteModel(FileStoreModel):

    root: pathlib.Path

    @override
    @asynccontextmanager
    async def open(self, config: Config):
        """Open a SQLite Archive."""
        self.root.mkdir(parents=True, exist_ok=True)
        # TODO: Instantiate and return the SQLite filestore
        # If the db doesn't exist, create it and initialize the schema.
        # Otherwise, confirm that it is a valid SQLite Archive.