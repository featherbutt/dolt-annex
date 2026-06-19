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

from dolt_annex.datatypes.async_utils import as_acm
import gallery_dl

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.replicated_db.dolt import DatabaseConnection, RepoDataset

config_path = Path(__file__).parent / "gallery_dl_config.json"
skip_db_path = Path(__file__).parent / "skip.sqlite3"

gdl_args = [ "gallery-dl", "--config", str(config_path) ]

@dataclass
class GalleryDLContext:
    repo: Repo
    repo_dataset: RepoDataset
    event_loop: asyncio.AbstractEventLoop
    submission_files_processed: int = 0
    submission_metadata_files_processed: int = 0
    post_metadata_files_processed: int = 0
    abort_flag: bool = False

    def run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.event_loop).result()

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

async def run_gallery_dl(config: Config, repo: Repo, batch_size: int, dataset_schema: DatasetSchema, capture_output: bool, *args) -> GalleryDLOutput:
    sys.argv = gdl_args + list(args)
    gallery_dl_stdout = io.StringIO()
    gallery_dl_stderr = io.StringIO()

    if not Path("skip.sqlite3").exists():
        shutil.copy(skip_db_path, "skip.sqlite3")
            

    async with (
        as_acm(DatabaseConnection.open(config)) as conn,
        as_acm(conn.open_dataset(dataset_schema)) as dataset,
        dataset.with_repo(repo.uuid) as repo_dataset,
    ):
        # gallery-dl is synchronous, so we need to run it in a separate thread, and use the
        # thread-safe queue.Queue to communicate tasks back to the async loop.
        loop = asyncio.get_running_loop()
        gallery_dl_context = GalleryDLContext(
            repo=repo,
            repo_dataset=repo_dataset,
            event_loop=loop
        )
        def gallery_dl_main():
            with (
                contextlib.ExitStack() as stack,
                with_gallery_dl_context(gallery_dl_context),
            ):
                if capture_output:
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