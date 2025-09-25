#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json
import os
from uuid import UUID
from typing_extensions import List

from .remote import Remote

@dataclass
class FileKeyTable:
    """
    All the information needed to diff file keys between two remotes.
    """
    name: str
    file_column: str
    key_columns: List[str]

    @staticmethod
    def from_name(name: str):
        # Look for a file in the current directory that matches the name.
        path = f"{name}.table"
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                if data.get("name") != name:
                    raise ValueError(f"Table name {data.get('name')} does not match expected name {name}")
                return FileKeyTable(**data)
        return None

    def insert_sql(self) -> str:
        cols = ", ".join([self.file_column] + self.key_columns)
        placeholders = ", ".join(["%s"] * (1 + len(self.key_columns)))
        return f"INSERT INTO {self.name} ({cols}) VALUES ({placeholders})"

@dataclass
class TableSettings:
    uuid: UUID
    table: FileKeyTable
    remote: Remote