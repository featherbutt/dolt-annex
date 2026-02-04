#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
gallery-dl integration for dolt-annex.
"""

import asyncio
from collections.abc import Awaitable
import contextlib
import contextvars
from dataclasses import dataclass
import io
import queue
import sys
from pathlib import Path
import threading

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
    tasks: queue.Queue[Awaitable]
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

async def run_gallery_dl(config: Config, repo: Repo, batch_size: int, dataset_schema: DatasetSchema, capture_output: bool, *args) -> GalleryDLOutput:
    sys.argv = gdl_args + list(args)
    gallery_dl_stdout = io.StringIO()
    gallery_dl_stderr = io.StringIO()

    async with Dataset.connect(config, db_batch_size=batch_size, dataset_schema=dataset_schema) as dataset:
        # gallery-dl is synchronous, so we need to run it in a separate thread, and use the
        # thread-safe queue.Queue to communicate tasks back to the async loop.
        tasks = queue.Queue[Awaitable]()
        gallery_dl_context = GalleryDLContext(repo=repo, dataset=dataset, tasks=tasks)
        def gallery_dl_main():
            with (
                contextlib.ExitStack() as stack,
                with_gallery_dl_context(gallery_dl_context),
            ):
                if capture_output:
                    stack.enter_context(contextlib.redirect_stdout(gallery_dl_stdout))
                    stack.enter_context(contextlib.redirect_stderr(gallery_dl_stderr))

                gallery_dl.main()
                tasks.shutdown()

        threading.Thread(target=gallery_dl_main).start()

        try:
            loop = asyncio.get_running_loop()
            while True:
                task = await loop.run_in_executor(None, tasks.get)
                await task
                tasks.task_done()
        except queue.ShutDown:
            pass

        return GalleryDLOutput(
            stdout=gallery_dl_stdout.getvalue(),
            stderr=gallery_dl_stderr.getvalue(),
            submission_files_processed=gallery_dl_context.submission_files_processed,
            submission_metadata_files_processed=gallery_dl_context.submission_metadata_files_processed,
            post_metadata_files_processed=gallery_dl_context.post_metadata_files_processed,
        )