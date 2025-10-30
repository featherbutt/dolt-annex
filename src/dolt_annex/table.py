#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
import os
import random
import time
from uuid import UUID
from typing_extensions import Callable, Dict, List, Tuple, Iterable

from dolt_annex.config.config import Config
from dolt_annex.datatypes.remote import Repo
from dolt_annex.datatypes.table import DatasetSchema, DatasetSource

from .dolt import DoltSqlServer
from .logger import logger
from .datatypes import AnnexKey, TableRow
from .datatypes.table import FileTableSchema
from dolt_annex import dolt

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
    flush_hooks: List[Callable[[], None]]
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

    def increment_count(self):
        self.count += 1
        if self.count >= self.batch_size:
            self.flush()
            self.count = 0

    def insert_file_source(self, table_row: TableRow, key: AnnexKey, source: UUID):
        if source not in self.added_rows:
            self.added_rows[source] = []
        self.added_rows[source].append((key, table_row))

        self.increment_count()

    def add_flush_hook[**P](self, hook: Callable[P, None], *args: P.args, **kwargs: P.kwargs) -> None:
        """Add a hook to be called when the cache is flushed."""
        self.flush_hooks.append(lambda: hook(*args, **kwargs))

    def flush(self):
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
            hook()

        num_keys = len(self.added_rows)
        self.added_rows.clear()

        new_now = time.time()
        elapsed_time = new_now - self.time
        logger.debug(f"added {num_keys} keys in {elapsed_time:.2f} seconds")
        self.time = new_now

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    def has_row(self, uuid: UUID, key: TableRow) -> bool:
        query_sql = f"SELECT 1 FROM `{self.dolt.db_name}/{uuid}-{self.dataset_name}`.{self.schema.name} WHERE " + " AND ".join([f"{col} = %s" for col, _ in zip(self.schema.key_columns, key)]) + " LIMIT 1"
        results = self.dolt.query(query_sql, tuple(key))
        for _ in results:
            return True
        return False
    
class Dataset:
    """A version controlled branch that contains one or more file tables."""
    name: str
    dataset_source: DatasetSource
    tables: Dict[str, FileTable]
    dolt: DoltSqlServer
    auto_push: bool

    MAX_EXTENSION_LENGTH = 4

    def __init__(self, base_config: Config, dolt: DoltSqlServer, dataset_schema: DatasetSchema, auto_push: bool, batch_size: int):
        self.name = dataset_schema.name
        self.dolt = dolt
        self.auto_push = auto_push
        self.tables = {table.name: FileTable(dolt, table, self.name, dataset_schema.empty_table_ref, auto_push, batch_size) for table in dataset_schema.tables}
        dolt.maybe_create_branch(f"{base_config.get_uuid()}-{self.name}", dataset_schema.empty_table_ref)

    def get_table(self, table_name: str) -> FileTable:
        return self.tables[table_name]
    
    def get_tables(self) -> Iterable[FileTable]:
        return self.tables.values()
    
    def pull_from(self, remote: Repo):
        self.dolt.pull_branch(f"{remote.uuid}-{self.name}", remote)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        for table in self.tables.values():
            table.__exit__(exc_type, exc_value, traceback)
        if self.auto_push:
            pass
            # self.dolt.push_branch()

    def flush(self):
        for table in self.tables.values():
            table.flush()

    @staticmethod
    @contextmanager
    def connect(base_config: Config, db_batch_size, dataset_schema: DatasetSchema):
        """Context manager for creating a Dataset object by connecting to the Dolt server."""
        # If configuration sets a port, use that.
        # Otherwise, use default port for connecting to an existing server and random port if we're spawning a new server.
        dolt_config = base_config.dolt
        port = dolt_config.port or (random.randint(20000, 30000) if dolt_config.spawn_dolt_server else 3306)
        db_config = {
            "user": dolt_config.user,
            "database": dolt_config.db_name,
            "autocommit": dolt_config.autocommit,
            "port": port,
            **dolt_config.connection,
        }
        if os.name == 'nt':
            db_config["host"] = dolt_config.hostname
        else:
            db_config["unix_socket"] = dolt_config.server_socket

        with (
            DoltSqlServer(dolt_config.dolt_dir, dolt_config.db_name, db_config, dolt_config.spawn_dolt_server) as dolt_server,
            Dataset(base_config, dolt_server, dataset_schema, False, db_batch_size) as dataset
        ):
            yield dataset