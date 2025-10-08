#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from typing_extensions import List

from .loader import Loadable
from .remote import Repo

@dataclass
class FileTableSchema(Loadable("table")):
    """
    The schema describing a table with a file column.
    Contains all the information needed to diff file keys between two remotes.
    """
    name: str
    file_column: str
    key_columns: List[str]

    def insert_sql(self) -> str:
        """
        Returns the SQL statement to insert a row into the table.
        """
        cols = ", ".join([self.file_column] + self.key_columns)
        placeholders = ", ".join(["%s"] * (1 + len(self.key_columns)))
        return f"INSERT INTO {self.name} ({cols}) VALUES ({placeholders})"
    
@dataclass
class DatasetSchema(Loadable("dataset")):
    """
    The schema describing one or more tables that are version controlled together.
    Contains all the information needed to diff file keys between two remotes.
    """
    name: str
    tables: List[FileTableSchema]
    empty_table_ref: str

    def get_table(self, table_name: str) -> FileTableSchema:
        for table in self.tables:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found in dataset {self.name}")


@dataclass
class DatasetSource:
    """
    A specific copy of a dataset, stored in a specific repo.
    """
    schema: DatasetSchema
    repo: Repo
    