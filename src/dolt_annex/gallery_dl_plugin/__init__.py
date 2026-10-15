#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
gallery-dl integration for dolt-annex.
"""

import asyncio
import contextlib
import contextvars
from dataclasses import dataclass
import logging
import shutil
import sys
from pathlib import Path

import gallery_dl

from dolt_annex.config.base import Config
from dolt_annex.config.gallerydl_config import GalleryDLConfig
from dolt_annex.gallery_dl_plugin.context import GalleryDLContext
from dolt_annex.interactive.ensure_gallery_dl_config import gallery_dl_config_file_location


from dolt_annex.datatypes.file_io import DevNull
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.replicated_db.dolt import Dataset

config_path = Path(__file__).parent / "gallery_dl_config.json"
skip_db_path = Path(__file__).parent / "skip.sqlite3"

gdl_args = [ "gallery-dl", "--config", str(config_path), "--warning" ]

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
    submission_files_processed: int = 0
    submission_metadata_files_processed: int = 0
    post_metadata_files_processed: int = 0

async def run_gallery_dl(
    config: Config,
    repo: Repo,
    dataset: Dataset,
    *args,
    username: str | None = None,
    password: str | None = None,
) -> GalleryDLOutput:
    def gdl_log_level_flags(log_level: str) -> list[str]:
        match log_level.upper():
            case "WARNING":
                return ["--warning"]
            case "ERROR", "CRITICAL":
                return ["--quiet"]
            case "DEBUG":
                return ["--verbose"]
            case "INFO":
                return []
            case _:
                return []

    gallery_dl_config: GalleryDLConfig = config.gallery_dl
    sys_args = gdl_args + gdl_log_level_flags(config.log_level)
    if gallery_dl_config_file_location.exists():
        sys_args += ["--config", str(gallery_dl_config_file_location)]
    if username:
        sys_args += ["--username", username]
    if password:
        sys_args += ["--password", password]
    sys_args += list(args)
    sys.argv = sys_args

    if not Path("skip.sqlite3").exists():
        shutil.copy(skip_db_path, "skip.sqlite3")
            

    loop = asyncio.get_running_loop()
    async with (
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
                if not gallery_dl_config.capture_output:
                    stack.enter_context(contextlib.redirect_stdout(DevNull()))

                # Clear gallery_dl's internal state to avoid interference between runs.
                gallery_dl.config.clear()

                # gallery-dl sets the root logger in a pretty hacky way, adding an extra handler without modifying existing ones.
                # To get normal behavior, we need to manually clear the root logger's handlers before running gallery-dl,
                # and reset the logger afterward.
                logging.getLogger().handlers.clear()
                gallery_dl.main()
                logging.basicConfig(level=config.log_level.upper(), force=True)

                if not gallery_dl_context.exception.done():
                    gallery_dl_context.exception.set_result(None)

        loop.run_in_executor(None, gallery_dl_main)

        await gallery_dl_context.exception
 
        return GalleryDLOutput(
            submission_files_processed=gallery_dl_context.submission_files_processed,
            submission_metadata_files_processed=gallery_dl_context.submission_metadata_files_processed,
            post_metadata_files_processed=gallery_dl_context.post_metadata_files_processed,
        )