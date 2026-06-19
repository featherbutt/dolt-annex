#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
from typing_extensions import List

from .loader import Loadable

class FileTableSchema(Loadable, extension="schema", config_dir=pathlib.Path(".")):
    """
    The schema describing a table with a file column.
    Contains all the information needed to diff file keys between two remotes.
    """
    name: str
    file_column: str
    key_columns: List[str]

    def all_columns(self):
        if self.file_column in self.key_columns:
            return self.key_columns
        return self.key_columns + [self.file_column]

class DatasetSchema(Loadable, extension="dataset", config_dir=pathlib.Path(".")):
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
