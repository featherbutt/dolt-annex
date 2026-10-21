#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import contextlib
import sys
from dataclasses import dataclass
from typing import TextIO

from dolt_annex.config.gallerydl_config import GalleryDLConfig
from dolt_annex.datatypes.repo import Repo
from dolt_annex.replicated_db.interface import DatasetReplica, ReplicatedDataset
from dolt_annex.writer import CollectionTables, ReplicaWriter

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
    metadata_writer: ReplicaWriter
    submission_writer: ReplicaWriter
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
            replicated_submissions_table = await exit_stack.enter_async_context(dataset.with_table("submissions"))
            replicated_metadata_table = await exit_stack.enter_async_context(dataset.with_table("metadata"))
            for collection in config.collections:
                collection_dataset = await exit_stack.enter_async_context(dataset.with_repo(collection.uuid))
                
                collections_submission_tables.append(collection_dataset.get_table("submissions"))
                collections_metadata_tables.append(collection_dataset.get_table("metadata"))
            collections_submission_tables=CollectionTables(collections_submission_tables)
            collections_metadata_tables=CollectionTables(collections_metadata_tables)
            metadata_table_replica = await exit_stack.enter_async_context(replicated_metadata_table.with_repo(repo.uuid))
            submission_table_replica = await exit_stack.enter_async_context(replicated_submissions_table.with_repo(repo.uuid))
            yield cls(
                config=config,
                repo=repo,
                repo_dataset=repo_dataset,
                event_loop=event_loop,
                task_group=task_group,
                dataset=dataset,
                exception=asyncio.Future(),
                metadata_writer=ReplicaWriter(metadata_table_replica, repo, collections_metadata_tables),
                submission_writer=ReplicaWriter(submission_table_replica, repo, collections_submission_tables),
            )