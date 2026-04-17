#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import logging
import os
import random
import time
from uuid import UUID
from typing_extensions import Any, Awaitable, Optional, Callable, Dict, List, Tuple, Iterable

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.table import DatasetSchema

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
class TableFilter:
    column_name: str
    column_value: Any
    
class FileTable:
    """A table that exists on mutliple remotes. Allows for batched operations against the Dolt database."""
    repo_dataset: RepoDataset
    urls: Dict[str, List[str]]
    sources: Dict[FileKey, List[str]]
    added_rows: List[Tuple[FileKey, TableRow]]
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

    async def insert_file_source(self, table_row: TableRow, key: FileKey):
        self.added_rows.append((key, table_row))

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

        branch = f"{self.uuid}-{self.dataset.name}"
        rows = self.added_rows
        with self.dolt.maybe_create_branch(branch, self.branch_start_point):
            if self.schema.file_column in self.schema.key_columns:
                self.dolt.executemany(self.schema.insert_sql(), [row[1] for row in rows])
            else:
                self.dolt.executemany(self.schema.insert_sql(), [(row[0], *row[1]) for row in rows])

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

    def has_row(self, key: TableRow) -> bool:
        query_sql = f"SELECT 1 FROM `{self.dolt.db_name}/{self.uuid}-{self.dataset.name}`.{self.schema.name} WHERE " + " AND ".join([f"{col} = %s" for col, _ in zip(self.schema.key_columns, key)]) + " LIMIT 1"
        results = self.dolt.query(query_sql, tuple(key))
        for _ in results:
            return True
        return False

    def get_row(self, key: TableRow) -> Optional[bytes]:
        query_sql = f"SELECT {self.schema.file_column} FROM `{self.dolt.db_name}/{self.uuid}-{self.dataset.name}`.{self.schema.name} WHERE " + " AND ".join([f"{col} = %s" for col, _ in zip(self.schema.key_columns, key)]) + " LIMIT 1"
        results = self.dolt.query(query_sql, tuple(key))
        for result in results:
            return result[0]
        return None
    
    def get_rows(self, *, columns: List[str] = [], filters: List[TableFilter] = []) -> Iterable[Tuple]:
        if not columns:
            columns = [self.schema.file_column] + self.schema.key_columns
        query_sql = f"SELECT {', '.join(columns)} FROM `{self.dolt.db_name}/{self.uuid}-{self.dataset.name}`.{self.schema.name}"
        if filters:
            query_sql += " WHERE " + " AND ".join([f"{f.column_name} = %s" for f in filters])
            params = tuple(f.column_value for f in filters)
        else:
            params = ()
        results = self.dolt.query(query_sql, params)
        yield from results
    
@dataclass
class DatabaseConnection:
    """
    A connection to a database that contains datasets.
    """
    dolt: DoltSqlServer

    @staticmethod
    @contextmanager
    def connect(base_config: Config):
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
            conn = DatabaseConnection(dolt_server)
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
            to_{file_key_table.file_column}, `diff_type`, {",".join("to_" + col for col in file_key_table.key_columns)}
        FROM dolt_commit_diff_{file_key_table.name}
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s)
        {''.join(f" AND to_{f.column_name} = %s" for f in filters)}
        """

class Dataset:
    """
    A dataset that contains one or more file tables.
    """
    conn: DatabaseConnection
    name: str
    schema: DatasetSchema

    def __init__(self, conn: DatabaseConnection, schema: DatasetSchema):
        self.conn = conn
        self.name = schema.name
        self.schema = schema
        
    @asynccontextmanager
    async def with_repo(self, repo: UUID):
        repo_dataset = RepoDataset(self, repo)
        try:
            yield repo_dataset
        finally:
            await repo_dataset.flush()
        
    def diff_keys(self, in_ref: UUID, not_in_ref: UUID, file_key_table: FileTableSchema, filters: List[TableFilter], limit: Optional[int] = None) -> Iterable[Tuple[str, FileKey, TableRow]]:
        refs = [in_ref, not_in_ref]
        refs.sort()
        union_branch_name = f"union-{refs[0]}-{refs[1]}-{self.name}"
        
        in_ref_branch = f"{in_ref}-{self.name}"
        not_in_ref_branch = f"{not_in_ref}-{self.name}"

        dolt = self.conn.dolt

        dolt.initialize_dataset_source(self.schema, in_ref)
        dolt.initialize_dataset_source(self.schema, not_in_ref)

        # Create the union branch if it doesn't exist
        # What if in_ref_branch hasn't been created yet? We need an approach that abstracts this away.
        # Don't pass branch names around as strings, pass them as first class objects.
        with self.conn.dolt.maybe_create_branch(union_branch_name, in_ref_branch):
            dolt.merge(in_ref_branch)
            dolt.merge(not_in_ref_branch)
            query = diff_query(file_key_table, filters)
            if limit is not None:
                query += " LIMIT %s"
                query_results = dolt.query(query, (not_in_ref_branch, union_branch_name, limit))
            else:
                query_results = dolt.query(query, (not_in_ref_branch, union_branch_name))
            # TODO: Wrap this in a helper function
            for (annex_key, diff_type, *key_parts) in query_results:
                yield (diff_type, FileKey.must_parse(bytes(annex_key, encoding='utf-8')), TableRow(tuple(key_parts)))

class RepoDataset:
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
        dataset.conn.dolt.initialize_dataset_source(self.dataset.schema, repo)

    def get_table(self, table_name: str) -> FileTable:
        return self.tables[table_name]
    
    def get_tables(self) -> Iterable[FileTable]:
        return self.tables.values()
    
    async def flush(self):
        for table in self.tables.values():
            await table.flush()

