#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
import random
import time
from uuid import UUID
from typing_extensions import Any, Awaitable, Optional, Callable, Dict, List, Tuple, Iterable

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema

from .dolt import DoltSqlServer
from .logger import logger
from .datatypes import AnnexKey, TableRow
from .datatypes.table import FileTableSchema

# We must prevent data loss in the event the process is interrupted:
# - Original file names contain data that is lost when the file is added to the annex
# - Adding a file to the annex without updating the database can result in the file being effectively lost
# - The context manager ensures that the database cache will be flushed if the process is terminated, but this is not sufficient
#   in the event of SIGKILL, power loss, or other catastrophic failure, or if the flush fails.
# - But if we commit the database entries before adding the annex files, if the files don't get moved and we might not re-add them.
# - But we can just check what files remain in the import directoy.
# - So we have a separate branch
# The safe approach is the following:
# - Add the database entries
# - After flushing the database cache, compute the new git-annex branch.
# - Move the annex files in a batch.

@dataclass
class TableFilter:
    column_name: str
    column_value: Any
    
class FileTable:
    """A table that exists on mutliple remotes. Allows for batched operations against the Dolt database."""
    urls: Dict[str, List[str]]
    sources: Dict[AnnexKey, List[str]]
    added_rows: Dict[UUID, List[Tuple[AnnexKey, TableRow]]]
    dolt: DoltSqlServer
    auto_push: bool
    batch_size: int
    count: int
    time: float
    flush_hooks: List[Callable[[], Awaitable[None]]]
    write_sources_table: bool = False
    write_git_annex: bool = False
    schema: FileTableSchema
    dataset_name: str
    branch_start_point: str

    def __init__(self, dolt: DoltSqlServer, schema: FileTableSchema, dataset_name: str, branch_start_point: str, auto_push: bool, batch_size: int):
        self.schema = schema
        self.dataset_name = dataset_name
        self.dolt = dolt
        self.flush_hooks = []
        self.added_rows = {}
        self.batch_size = batch_size
        self.count = 0
        self.time = time.time()
        self.auto_push = auto_push
        self.branch_start_point = branch_start_point

    async def increment_count(self):
        self.count += 1
        if self.count >= self.batch_size:
            await self.flush()
            self.count = 0

    async def insert_file_source(self, table_row: TableRow, key: AnnexKey, source: UUID):
        if source not in self.added_rows:
            self.added_rows[source] = []
        self.added_rows[source].append((key, table_row))

        await self.increment_count()

    def add_flush_hook[**P](self, hook: Callable[P, Awaitable[None]], *args: P.args, **kwargs: P.kwargs) -> None:
        """Add a hook to be called when the cache is flushed."""
        self.flush_hooks.append(lambda: hook(*args, **kwargs))

    async def flush(self):
        """Flush the cache to the git-annex branch and the Dolt database."""
        # Flushing the cache must be done in the following order:
        # 1. Update the git-annex branch to contain the new ownership records and registered urls.
        # 2. Update the Dolt database to match the git-annex branch.
        # 3. Move the annex files to the annex directory. This step is a no-op when running the downloader,
        #    because downloaded files were already written into the annex.
        # This way, if the import process is interrupted, all incomplete files will still exist in the source directory.
        # Likewise, if a download process is interrupted, the database will still indicate which files have been downloaded.

        for source, rows in self.added_rows.items():
            branch = f"{source}-{self.dataset_name}"
            with self.dolt.maybe_create_branch(branch, self.branch_start_point):
                self.dolt.executemany(self.schema.insert_sql(), [(row[0], *row[1]) for row in rows])

        for hook in self.flush_hooks:
            await hook()

        num_keys = len(self.added_rows)
        self.added_rows.clear()

        new_now = time.time()
        elapsed_time = new_now - self.time
        logger.debug(f"added {num_keys} keys in {elapsed_time:.2f} seconds")
        self.time = new_now

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.flush()

    def has_row(self, uuid: UUID, key: TableRow) -> bool:
        query_sql = f"SELECT 1 FROM `{self.dolt.db_name}/{uuid}-{self.dataset_name}`.{self.schema.name} WHERE " + " AND ".join([f"{col} = %s" for col, _ in zip(self.schema.key_columns, key)]) + " LIMIT 1"
        results = self.dolt.query(query_sql, tuple(key))
        for _ in results:
            return True
        return False

    def get_row(self, uuid: UUID, key: TableRow) -> Optional[bytes]:
        query_sql = f"SELECT {self.schema.file_column} FROM `{self.dolt.db_name}/{uuid}-{self.dataset_name}`.{self.schema.name} WHERE " + " AND ".join([f"{col} = %s" for col, _ in zip(self.schema.key_columns, key)]) + " LIMIT 1"
        results = self.dolt.query(query_sql, tuple(key))
        for result in results:
            return result[0]
        return None
    
    def get_rows(self, uuid: UUID, filters: List[TableFilter]) -> Iterable[Tuple[TableRow, bytes]]:
        query_sql = f"SELECT {self.schema.file_column}, " + ", ".join(self.schema.key_columns) + f" FROM `{self.dolt.db_name}/{uuid}-{self.dataset_name}`.{self.schema.name}"
        if filters:
            query_sql += " WHERE " + " AND ".join([f"{f.column_name} = %s" for f in filters])
            params = tuple(f.column_value for f in filters)
        else:
            params = ()
        results = self.dolt.query(query_sql, params)
        yield from results
    
class Dataset:
    """A version controlled branch that contains one or more file tables."""
    name: str
    schema: DatasetSchema
    tables: Dict[str, FileTable]
    dolt: DoltSqlServer
    auto_push: bool

    MAX_EXTENSION_LENGTH = 4

    def __init__(self, base_config: Config, dolt: DoltSqlServer, schema: DatasetSchema, auto_push: bool, batch_size: int):
        self.name = schema.name
        self.schema = schema
        self.dolt = dolt
        self.auto_push = auto_push
        self.tables = {table.name: FileTable(dolt, table, self.name, schema.empty_table_ref, auto_push, batch_size) for table in schema.tables}
        dolt.maybe_create_branch(f"{base_config.get_uuid()}-{self.name}", schema.empty_table_ref)

    def get_table(self, table_name: str) -> FileTable:
        return self.tables[table_name]
    
    def get_tables(self) -> Iterable[FileTable]:
        return self.tables.values()
    
    def pull_from(self, remote: Repo):
        self.dolt.pull_branch(f"{remote.uuid}-{self.name}", remote)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        for table in self.tables.values():
            await table.__aexit__(exc_type, exc_value, traceback)
        if self.auto_push:
            pass
            # self.dolt.push_branch()

    async def flush(self):
        for table in self.tables.values():
            await table.flush()

    @staticmethod
    @asynccontextmanager
    async def connect(base_config: Config, db_batch_size, dataset_schema: DatasetSchema):
        """Context manager for creating a Dataset object by connecting to the Dolt server."""
        # If configuration sets a port, use that.
        # Otherwise, use default port for connecting to an existing server and random port if we're spawning a new server.
        dolt_config = base_config.dolt
        connection = dolt_config.connection
        db_config = {
            "user": dolt_config.connection.user,
            "database": dolt_config.connection.database,
            "autocommit": dolt_config.connection.autocommit,
            **dolt_config.connection.extra_params,
        }
        if os.name != 'nt' and connection.server_socket:
            db_config["unix_socket"] = connection.server_socket.as_posix()
        elif connection.hostname:
            db_config["host"] = connection.hostname
            port = random.randint(20000, 30000) if dolt_config.spawn_dolt_server else (connection.port or 3306)
            db_config["port"] = port
        else:
            raise ValueError("Either server_socket or hostname must be set in the Dolt connection configuration.")

        with (
            DoltSqlServer(dolt_config.dolt_dir, connection.database, db_config, dolt_config.spawn_dolt_server) as dolt_server,
        ):
            async with Dataset(base_config, dolt_server, dataset_schema, False, db_batch_size) as dataset:
                yield dataset