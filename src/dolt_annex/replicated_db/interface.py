"""
Instead of interacting with Dolt directly, we access an interface that Dolt implements.

This is because there are multiple possible ways to use Dolt to implement the necessary features.

Dolt concepts like branches and commit history get abstracted away.

For example, remote-dataset pairs need to map onto branches.

There are multiple copies of each dataset, one for each repo.

One of the current obstacles is that we need branches in order to merge.

An abstract "versioned database" provides a way to diff and merge different versions of a table without directly using Dolt.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Optional, Self, Tuple
from typing_extensions import Any

from dolt_annex.datatypes.async_types import AsyncContextManager, ContextManager
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.file_keys.base import FileKey

@dataclass
class TableFilter:
    column_name: str
    column_value: Any

class ReplicatedDatabase(ABC):

    @classmethod
    @abstractmethod
    def open(cls, base_config: Config) -> ContextManager[Self]:
        """
        Open a connection to a replicated database.
        """
        
    @abstractmethod
    def open_dataset(self, dataset_schema: DatasetSchema) -> ContextManager[ReplicatedDataset]:
        """
        Open a specific dataset stored in the database.
        """
        
class ReplicatedDataset(ABC):
    @abstractmethod
    def with_repo(self, repo: Repo.Id) -> AsyncContextManager[DatasetReplica]:
        """
        Return the replica of this dataset from a specific repo.
        """

    @abstractmethod
    def with_table(self, table_name: str) -> AsyncContextManager[ReplicatedTable]:
        """
        Open a specific table in the dataset.
        """

class ReplicatedTable(ABC):
    @abstractmethod
    def diff_keys(self, in_repo: Repo.Id, not_in_repo: Repo.Id, filters: List[TableFilter], limit: Optional[int] = None) -> Iterable[Tuple[str, FileKey, TableRow, TableRow]]:
        """
        Return an iterable containing all the rows in this table that appear in one repo but not the other.
        """

    @abstractmethod
    def with_repo(self, repo: Repo.Id) -> AsyncContextManager[TableReplica]:
        """
        Return the replica of this table from a specific repo.
        """

class DatasetReplica(ABC):
    """
    A DatasetReplica represents a specific copy of a ReplicatedDataset in a specific repo.
    """

    @property
    @abstractmethod
    def dataset(self) -> ReplicatedDataset:
        """
        Return the ReplicatedDataset that this replica belongs to.
        """

    @abstractmethod
    def get_table(self, table_name: str) -> TableReplica:
        """
        Open a replica of this table from a specific repo.
        """

    @abstractmethod
    def get_tables(self) -> Iterable[TableReplica]:
        """
        Return an iterable of all the table replicas in this dataset replica.
        """

class TableReplica(ABC):
    schema: FileTableSchema
    table: ReplicatedTable

    def has_row(self, filters: Iterable[TableFilter] = ()) -> bool:
        """
        Return whether this replica contains a row for the provided key.
        """
        for _ in self.get_rows(filters=filters):
            return True
        return False

    def get_row(self, *, columns: Iterable[str] = (), filters: Iterable[TableFilter] = ()) -> Optional[TableRow]:
        """
        Returns the row for the provided key, if it exists.
        """
        for result in self.get_rows(columns=columns, filters=filters):
            return result
        return None
    
    @abstractmethod
    def get_rows(self, *, columns: Iterable[str] = (), filters: Iterable[TableFilter] = ()) -> Iterable[TableRow]:
        """
        Returns an iterable of all rows in the table that match the provided filters.
        """

    @abstractmethod
    async def insert(self, table_row: TableRow):
        """
        Inserts a row into the table. This row is not guarenteed to be persisted until the ReplicatedDatabase is closed,
        or until flush() is called.
        """


    @abstractmethod
    async def remove(self, table_row: TableRow):
        """
        Removes a row from the table. This row is not guarenteed to be persisted until the ReplicatedDatabase is closed,
        or until flush() is called.
        """

    @abstractmethod
    async def flush(self):
        """
        Flushes any pending changes to the database.
        """