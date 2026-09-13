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
import shutil
import sys
from pathlib import Path
from typing import List

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.config.gallerydl_config import GalleryDLConfig
import gallery_dl

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import DatasetReplica, ReplicatedDataset, TableReplica

config_path = Path(__file__).parent / "gallery_dl_config.json"
skip_db_path = Path(__file__).parent / "skip.sqlite3"

gdl_args = [ "gallery-dl", "--config", str(config_path) ]

@dataclass
class GalleryDLContext:
    config: GalleryDLConfig
    repo: Repo
    repo_dataset: DatasetReplica
    event_loop: asyncio.AbstractEventLoop
    collections_submission_tables: CollectionTables
    collections_metadata_tables: CollectionTables
    submission_files_processed: int = 0
    submission_metadata_files_processed: int = 0
    post_metadata_files_processed: int = 0
    abort_flag: bool = False

    @dataclass
    class CollectionTables:
        tables: List[TableReplica]

        async def insert(self, row: TableRow):
            for table in self.tables:
                await table.insert(row)


    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.event_loop).result()

    @classmethod
    @contextlib.asynccontextmanager
    async def new(
        cls,
        dataset: ReplicatedDataset,
        config: GalleryDLConfig,
        repo: Repo,
        event_loop: asyncio.AbstractEventLoop,
    ):
        collections_submission_tables = []
        collections_metadata_tables = []
        async with contextlib.AsyncExitStack() as exit_stack:
            repo_dataset = await exit_stack.enter_async_context(dataset.with_repo(repo.uuid))
            for collection in config.collections:
                collection_dataset = await exit_stack.enter_async_context(dataset.with_repo(collection.uuid))
                collections_submission_tables.append(collection_dataset.get_table("submissions"))
                collections_metadata_tables.append(collection_dataset.get_table("metadata"))
            yield cls(
                config=config,
                repo=repo,
                repo_dataset=repo_dataset,
                event_loop=event_loop,
                collections_submission_tables=GalleryDLContext.CollectionTables(collections_submission_tables),
                collections_metadata_tables=GalleryDLContext.CollectionTables(collections_metadata_tables),
            )

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
                key_columns=["source", "id", "metadata_file_key", "part", "submission_file_key"],
                file_column="submission_file_key",
            ),
            FileTableSchema(
                name="metadata",
                key_columns=["source", "id", "file_key"],
                file_column="file_key",
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

async def run_gallery_dl(config: Config, repo: Repo, gallery_dl_config: GalleryDLConfig, dataset_schema: DatasetSchema, *args) -> GalleryDLOutput:
    sys.argv = gdl_args + list(args)
    gallery_dl_stdout = io.StringIO()
    gallery_dl_stderr = io.StringIO()

    if not Path("skip.sqlite3").exists():
        shutil.copy(skip_db_path, "skip.sqlite3")
            

    loop = asyncio.get_running_loop()
    async with (
        as_acm(DatabaseConnection.open(config)) as conn,
        as_acm(conn.open_dataset(dataset_schema)) as dataset,
        GalleryDLContext.new(
            dataset=dataset,
            config=gallery_dl_config,
            repo=repo,
            event_loop=loop
        ) as gallery_dl_context
    ):
        # gallery-dl is synchronous, so we need to run it in a separate thread, and use the
        # thread-safe queue.Queue to communicate tasks back to the async loop.
        def gallery_dl_main():
            with (
                contextlib.ExitStack() as stack,
                with_gallery_dl_context(gallery_dl_context),
            ):
                if gallery_dl_config.capture_output:
                    stack.enter_context(contextlib.redirect_stdout(gallery_dl_stdout))
                    stack.enter_context(contextlib.redirect_stderr(gallery_dl_stderr))

                # Clear gallery_dl's internal state to avoid interference between runs.
                gallery_dl.config.clear()
                gallery_dl.main()

        gallery_dl_thread = loop.run_in_executor(None, gallery_dl_main)

        await gallery_dl_thread

        return GalleryDLOutput(
            stdout=gallery_dl_stdout.getvalue(),
            stderr=gallery_dl_stderr.getvalue(),
            submission_files_processed=gallery_dl_context.submission_files_processed,
            submission_metadata_files_processed=gallery_dl_context.submission_metadata_files_processed,
            post_metadata_files_processed=gallery_dl_context.post_metadata_files_processed,
        )