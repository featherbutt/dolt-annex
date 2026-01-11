#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
gallery-dl integration for dolt-annex.
"""

import asyncio
import contextlib
import contextvars
from dataclasses import dataclass
import io
import sys
from pathlib import Path

import gallery_dl

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.table import Dataset

config_path = Path(__file__).parent / "gallery_dl_config.json"
skip_db_path = Path(__file__).parent / "skip.sqlite3"

gdl_args = [ "gallery-dl", "--config", str(config_path) ]

@dataclass
class GalleryDLContext:
    repo: Repo
    dataset: Dataset
    tasks: asyncio.TaskGroup
    submission_files_processed: int = 0
    submission_metadata_files_processed: int = 0
    post_metadata_files_processed: int = 0

_gallery_dl_context = contextvars.ContextVar[GalleryDLContext]("gallery_dl_context")

@contextlib.contextmanager
def with_gallery_dl_context(context: GalleryDLContext):
    token = _gallery_dl_context.set(context)
    try:
        yield
    finally:
        _gallery_dl_context.reset(token)

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

@dataclass
class GalleryDLOutput:
    stdout: str
    stderr: str
    submission_files_processed: int = 0
    submission_metadata_files_processed: int = 0
    post_metadata_files_processed: int = 0

async def run_gallery_dl(config: Config, repo: Repo, batch_size: int, dataset_schema: DatasetSchema, *args) -> GalleryDLOutput:
    sys.argv = gdl_args + list(args)
    gallery_dl_stdout = io.StringIO()
    gallery_dl_stderr = io.StringIO()

    async with Dataset.connect(config, db_batch_size=batch_size, dataset_schema=dataset_schema) as dataset:
        async with asyncio.TaskGroup() as tasks:
            gallery_dl_context = GalleryDLContext(repo=repo, dataset=dataset, tasks=tasks)
            with (
                contextlib.redirect_stdout(gallery_dl_stdout),
                contextlib.redirect_stderr(gallery_dl_stderr),
                with_gallery_dl_context(gallery_dl_context),
            ):
                gallery_dl.main()
        return GalleryDLOutput(
            stdout="",
            stderr="",
            submission_files_processed=gallery_dl_context.submission_files_processed,
            submission_metadata_files_processed=gallery_dl_context.submission_metadata_files_processed,
            post_metadata_files_processed=gallery_dl_context.post_metadata_files_processed,
        )