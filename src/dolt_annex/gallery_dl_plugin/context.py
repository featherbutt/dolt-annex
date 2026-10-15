#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import contextlib
import sys
from dataclasses import dataclass
from typing import List, TextIO

from dolt_annex.datatypes.common import TableRow
from dolt_annex.config.gallerydl_config import GalleryDLConfig
from dolt_annex.datatypes.repo import Repo
from dolt_annex.replicated_db.interface import DatasetReplica, ReplicatedDataset, TableReplica

@dataclass
class GalleryDLContext:
    """
    The context for managing interactions with dolt-annex from within the gallery-dl thread.
    """
    config: GalleryDLConfig
    repo: Repo
    dataset: ReplicatedDataset
    repo_dataset: DatasetReplica
    event_loop: asyncio.AbstractEventLoop
    collections_submission_tables: CollectionTables
    collections_metadata_tables: CollectionTables
    task_group: asyncio.TaskGroup
    exception: asyncio.Future[None]
    stdout: TextIO = sys.stdout
    submission_files_processed: int = 0
    submission_metadata_files_processed: int = 0
    post_metadata_files_processed: int = 0

    # Because gallery-dl is single-threaded, it will only write one file at a time.
    # We expect that files will be read by the dolt-annex thread faster than they are written
    # by the gallery-dl thread, but we use a semaphore as a precaution to limit the number of
    # in-flight files.
    semaphore: asyncio.Semaphore = asyncio.Semaphore(4)
    abort_flag: bool = False

    @dataclass
    class CollectionTables:
        tables: List[TableReplica]

        async def insert(self, row: TableRow):
            for table in self.tables:
                await table.insert(row)


    def run(self, coro):
        """Run a coroutine in the gallery-dl event loop."""
        return asyncio.run_coroutine_threadsafe(coro, self.event_loop).result()

    def enqueue_task(self, coro):
        """Enqueue a coroutine to be run as a task in the gallery-dl event loop."""
        async def create_task():
            async with self.semaphore:
                self.task_group.create_task(coro)
        return asyncio.run_coroutine_threadsafe(create_task(), self.event_loop)

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
        async with (
            contextlib.AsyncExitStack() as exit_stack,
            asyncio.TaskGroup() as task_group,
        ):
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
                task_group=task_group,
                dataset=dataset,
                exception=asyncio.Future(),
                collections_submission_tables=GalleryDLContext.CollectionTables(collections_submission_tables),
                collections_metadata_tables=GalleryDLContext.CollectionTables(collections_metadata_tables),
            )