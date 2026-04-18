#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import logging
import os
import random
import time
from typing import Generator, Self
from uuid import UUID
from typing_extensions import Awaitable, Optional, Callable, Dict, List, Tuple, Iterable

from dolt_annex.database import TableFilter
from dolt_annex.datatypes.async_types import AsyncContextManager
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
import dolt_annex.database as database

from .dolt import DoltSqlServer
from .datatypes import FileKey, TableRow
from .datatypes.table import FileTableSchema

logger = logging.getLogger(__name__)

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
class DatabaseConnection(database.ReplicatedDatabase):
    """
    A connection to a database that contains datasets.
    """
    dolt: DoltSqlServer

    @classmethod
    @contextmanager
    def open(cls, base_config: Config) -> Generator[Self]:
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
            conn = cls(dolt_server)
            yield conn

    @contextmanager
    def open_dataset(self, dataset_schema: DatasetSchema):
        dataset = Dataset(self, dataset_schema)
        yield dataset

def diff_query(file_key_table: FileTableSchema, filters: List[TableFilter]) -> str:
    """
    Generates a SQL query to identify the files that exist on one remote but not another.
    Note that generating a SQL query this way is not safe from SQL injection, but SQL injection
    isn't part of the threat model, since any query that the application can run,
    the user can already run themselves.
    """
    return f"""
        SELECT
            `diff_type`, `{"to_" + file_key_table.file_column}`, {",".join("to_" + col for col in file_key_table.all_columns())}
        FROM dolt_commit_diff_{file_key_table.name}
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s)
        {''.join(f" AND to_{f.column_name} = %s" for f in filters)}
        """

class Dataset(database.ReplicatedDataset):
    """
    A dataset that contains one or more file tables.
    """
    conn: DatabaseConnection
    name: str
    schema: DatasetSchema

    @property
    def dolt(self) -> DoltSqlServer:
        return self.conn.dolt

    def __init__(self, conn: DatabaseConnection, schema: DatasetSchema):
        self.conn = conn
        self.name = schema.name
        self.schema = schema

    def initialize_dataset_source(self, dataset_schema: DatasetSchema, repo_uuid: UUID):
        """
        Ensures that the Dolt repo contains the necessary branches for this dataset.
        """
        self.dolt.maybe_create_branch(f"{repo_uuid}-{dataset_schema.name}", dataset_schema.empty_table_ref)

        
    def diff_keys(self, in_ref: UUID, not_in_ref: UUID, file_key_table: FileTableSchema, filters: List[TableFilter], limit: Optional[int] = None) -> Iterable[Tuple[str, FileKey, TableRow]]:
        refs = [in_ref, not_in_ref]
        refs.sort()
        union_branch_name = f"union-{refs[0]}-{refs[1]}-{self.name}"
        
        in_ref_branch = f"{in_ref}-{self.name}"
        not_in_ref_branch = f"{not_in_ref}-{self.name}"

        dolt = self.dolt

        self.initialize_dataset_source(self.schema, in_ref)
        self.initialize_dataset_source(self.schema, not_in_ref)

        # Create the union branch if it doesn't exist
        # What if in_ref_branch hasn't been created yet? We need an approach that abstracts this away.
        # Don't pass branch names around as strings, pass them as first class objects.
        with dolt.maybe_create_branch(union_branch_name, in_ref_branch):
            dolt.merge(in_ref_branch)
            dolt.merge(not_in_ref_branch)
            query = diff_query(file_key_table, filters)
            if limit is not None:
                query += " LIMIT %s"
                query_results = dolt.query(query, (not_in_ref_branch, union_branch_name, limit))
            else:
                query_results = dolt.query(query, (not_in_ref_branch, union_branch_name))
            # TODO: Wrap this in a helper function
            for (diff_type, annex_key, *key_parts) in query_results:
                table_row = {key: value for key, value in zip(file_key_table.all_columns(), key_parts)}
                yield (diff_type, FileKey.must_parse(bytes(annex_key, encoding='utf-8')), TableRow(table_row))

    @asynccontextmanager
    async def with_repo(self, repo: Repo.Id):
        repo_dataset = RepoDataset(self, repo)
        try:
            yield repo_dataset
        finally:
            await repo_dataset.flush()

    def with_table(self, table_name: str) -> AsyncContextManager:
        raise NotImplementedError

class RepoDataset(database.DatasetReplica):
    """
    The dataset as it exists on a specific repo. Every row in this dataset corresponds to a file on that repo.
    """
    dataset: Dataset
    repo: UUID
    tables: Dict[str, FileTable]

    def __init__(self, dataset: Dataset, repo: UUID):
        self.dataset = dataset
        self.repo = repo
        self.tables = {table.name: FileTable(self, table, dataset.schema.empty_table_ref) for table in dataset.schema.tables}
        dataset.initialize_dataset_source(self.dataset.schema, repo)

    def get_table(self, table_name: str) -> FileTable:
        return self.tables[table_name]
    
    def get_tables(self) -> Iterable[FileTable]:
        return self.tables.values()
    
    async def flush(self):
        for table in self.tables.values():
            await table.flush()

class FileTable(database.TableReplica):
    """A table that exists on mutliple remotes. Allows for batched operations against the Dolt database."""
    repo_dataset: RepoDataset
    urls: Dict[str, List[str]]
    sources: Dict[FileKey, List[str]]
    added_rows: List[TableRow]
    batch_size: int
    count: int
    time: float
    flush_hooks: List[Callable[[], Awaitable[None]]]
    write_sources_table: bool = False
    write_git_annex: bool = False
    schema: FileTableSchema
    branch_start_point: str

    def __init__(self, repo_dataset: RepoDataset, schema: FileTableSchema, branch_start_point: str):
        self.repo_dataset = repo_dataset
        self.schema = schema
        self.flush_hooks = []
        self.added_rows = []
        self.batch_size = 1000
        self.count = 0
        self.time = time.time()
        self.branch_start_point = branch_start_point

    @property
    def uuid(self) -> UUID:
        return self.repo_dataset.repo

    @property
    def dataset(self) -> Dataset:
        return self.repo_dataset.dataset
    
    @property
    def dolt(self) -> DoltSqlServer:
        return self.dataset.conn.dolt

    async def increment_count(self):
        self.count += 1
        if self.count >= self.batch_size:
            await self.flush()
            self.count = 0

    async def insert(self, table_row: TableRow):
        self.added_rows.append(table_row)

        await self.increment_count()

    def add_flush_hook[**P](self, hook: Callable[P, Awaitable[None]], *args: P.args, **kwargs: P.kwargs) -> None:
        """Add a hook to be called when the cache is flushed."""
        self.flush_hooks.append(lambda: hook(*args, **kwargs))

    async def flush(self):
        """Flush the cache to the Dolt database and execute callbacks (typically for importing files into filestores)."""
        # Flushing the cache must be done in the following order:
        # 1. Update the git-annex branch to contain the new ownership records and registered urls.
        # 2. Update the Dolt database to match the git-annex branch.
        # 3. Move the annex files to the annex directory. This step is a no-op when running the downloader,
        #    because downloaded files were already written into the annex.
        # This way, if the import process is interrupted, all incomplete files will still exist in the source directory.
        # Likewise, if a download process is interrupted, the database will still indicate which files have been downloaded.

        branch = f"{self.uuid}-{self.dataset.name}"
        rows = [[row[key] for key in self.schema.all_columns()] for row in self.added_rows]
        with self.dolt.maybe_create_branch(branch, self.branch_start_point):
            # pass in dict to insert, correctly make query here
            self.dolt.executemany(self.insert_sql(), rows)

        for hook in self.flush_hooks:
            await hook()

        num_keys = len(self.added_rows)
        self.added_rows.clear()

        new_now = time.time()
        elapsed_time = new_now - self.time
        logger.debug("added %d keys in %.2f seconds", num_keys, elapsed_time)
        self.time = new_now

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.flush()
    
    def get_rows(self, *, columns: Iterable[str] = (), filters: Iterable[TableFilter] = ()) -> Iterable[TableRow]:
        if not columns:
            columns = self.schema.all_columns()
        query_sql = f"SELECT {', '.join(columns)} FROM `{self.dolt.db_name}/{self.uuid}-{self.dataset.name}`.{self.schema.name}"
        if filters:
            query_sql += " WHERE " + " AND ".join([f"{f.column_name} = %s" for f in filters])
            params = tuple(f.column_value for f in filters)
        else:
            params = ()
        results = self.dolt.query(query_sql, params)
        for result in results:
            yield TableRow({key: value for (key, value) in zip(columns, result)})

    def insert_sql(self) -> str:
        """
        Returns the SQL statement to insert a row into the table.
        """
        cols = ", ".join(self.schema.all_columns())
        placeholders = ", ".join(["%s"] * (len(self.schema.all_columns())))
        return f"REPLACE INTO {self.schema.name} ({cols}) VALUES ({placeholders})"