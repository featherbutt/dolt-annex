#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
from pydantic import BaseModel
from typing_extensions import List

from .loader import Loadable

class FileTableSchema(Loadable, BaseModel, extension="schema", config_dir=pathlib.Path(".")):
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
        return f"REPLACE INTO {self.name} ({cols}) VALUES ({placeholders})"
    
class DatasetSchema(Loadable, BaseModel, extension="dataset", config_dir=pathlib.Path(".")):
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
