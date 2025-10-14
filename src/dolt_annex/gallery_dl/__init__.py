#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
gallery-dl integration for dolt-annex.
"""

import contextvars
import sys
from pathlib import Path

import gallery_dl

from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.table import Dataset

config_path = Path(__file__).parent / "config.json"
skip_db_path = Path(__file__).parent / "skip.sqlite3"

gdl_args = [ "gallery-dl", "--config", str(config_path) ]

dataset_context = contextvars.ContextVar[Dataset]("dataset")

def make_default_schema(dataset_name: str) -> DatasetSchema:
    return DatasetSchema(
        name=dataset_name,
        tables=[
            FileTableSchema(
                name="submissions",
                key_columns=["source", "id", "updated", "part"],
                        file_column="annex_key",
                    ),
                    FileTableSchema(
                        name="metadata",
                        key_columns=["source", "id", "updated"],
                        file_column="annex_key",
                    ),
                    FileTableSchema(
                        name="posts",
                key_columns=["source", "id", "updated"],
                file_column="annex_key",
            ),
        ],
        empty_table_ref="gallery-dl",
    )

def run_gallery_dl(*args):
    sys.argv = gdl_args + list(args)
    gallery_dl.main()