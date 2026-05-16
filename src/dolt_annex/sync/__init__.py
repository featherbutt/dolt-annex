#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import logging
from typing_extensions import Iterable, Optional, Tuple, List

from dolt_annex.replicated_db.dolt import Dataset, FileTable
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.datatypes import TableRow
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.async_utils import Result
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStoreError
from dolt_annex.filestore.cas import filestore_copy

logger = logging.getLogger(__name__)

class SyncOperation:
    to_table: FileTable
    from_repo: Repo
    to_repo: Repo
    ignore_missing: bool = False

    work_queue: asyncio.Queue[Optional[Tuple[FileKey, TableRow]]]
    files_moved: List[FileKey]
    pending_exceptions: List[Exception]

    def __init__(
            self,
            *,
            to_table: FileTable,
            from_repo: Repo,
            to_repo: Repo,
            ignore_missing: bool = False,
            queue_size: Optional[int] = 1000,
    ) -> None:
        self.to_table = to_table
        self.from_repo = from_repo
        self.to_repo = to_repo
        self.ignore_missing = ignore_missing
        self.work_queue = asyncio.Queue(maxsize=queue_size or 0)
        self.files_moved = []
        self.pending_exceptions = []
    async def worker_loop(self) -> None:
        while True:
            try:
                item = await self.work_queue.get()
            except asyncio.QueueShutDown:
                break
            if item is None:
                self.work_queue.task_done()
                break
            key, table_row = item
            try:
                result = await self.move_submission_and_key(
                    key,
                    table_row,
                )
                await result.wait_for_complete()
                self.files_moved.append(key)
            except (FileNotFoundError, FileStoreError) as e:
                self.pending_exceptions.append(e)
            finally:
                self.work_queue.task_done()

    async def move(self, where: List[TableFilter], batch_size: Optional[int] = None) -> None:

        has_more = True
        while has_more:
            keys_and_submissions = list(self.to_table.dataset.diff_keys(self.from_repo.uuid, self.to_repo.uuid, self.to_table.schema, where, batch_size))
            has_more = await self.move_submissions_and_keys(keys_and_submissions)
            # Await here so that the next diff sees the updated state
            await self.work_queue.join()
            if self.pending_exceptions:
                raise ExceptionGroup("exceptions during sync", self.pending_exceptions)
            await self.to_table.flush()

    async def move_submissions_and_keys(self, keys_and_submissions: Iterable[Tuple[str, FileKey, TableRow, TableRow]]) -> bool:
        has_more = False
        for diff_type, key, to_table_row, from_table_row in keys_and_submissions:
            has_more = True
            match diff_type:
                case "added": 
                    await self.work_queue.put((key, to_table_row))
                case "removed":
                    from_table_key = { column: from_table_row[column] for column in self.to_table.schema.key_columns }
                    await self.to_table.remove(from_table_key)
                case "modified":
                    await self.work_queue.put((key, to_table_row))
                    from_table_key = { column: from_table_row[column] for column in self.to_table.schema.key_columns }
                    await self.to_table.remove(from_table_key)
                case _:
                    raise ValueError(f"Unknown diff type {diff_type}")
                    
        return has_more
    
    async def move_submission_and_key(self, key: FileKey, table_row: TableRow) -> Result[None]:
        logger.info("moving %s: %s", table_row, key)

        if await maybe_await(self.to_repo.filestore.exists(key)):
            logger.debug("file %s already exists in destination filestore", key)
            # The file may have come from a different dataset, so we don't need to copy it.
            # We still record that we have a copy of it for this dataset.
            await self.to_table.insert(table_row)
            return Result.done()
        if self.ignore_missing and not await maybe_await(self.from_repo.filestore.exists(key)):
            logger.debug("Missing file %s in source filestore, skipping due to --ignore-missing", key)
            return Result.done()
        result = await filestore_copy(src=self.from_repo.filestore, dst=self.to_repo.filestore, key=key)
        # We must wait for the copy to complete before updating the dataset.
        async def update_table_on_complete() -> None:
            await result.wait_for_complete()
            await self.to_table.insert(table_row)
        return Result(asyncio.create_task(update_table_on_complete()))

    @classmethod
    @asynccontextmanager
    async def context_manager(
            cls,
            *,
            to_table: FileTable,
            from_repo: Repo,
            to_repo: Repo,
            ignore_missing: bool = False,
            num_workers: int = 4,
            queue_size: Optional[int] = 1000,
    ) -> AsyncGenerator[SyncOperation, None]:
        async with (
            asyncio.TaskGroup() as workers
        ):
            sync_op = cls(
                to_table = to_table,
                from_repo=from_repo,
                to_repo=to_repo,
                ignore_missing=ignore_missing,
                queue_size=queue_size,
            )
            for _ in range(num_workers):
                workers.create_task(sync_op.worker_loop())
            try:
                yield sync_op
            finally:
                for _ in range(num_workers):
                    await sync_op.work_queue.put(None)
    
class FileModifiedError(Exception):
    def __init__(self, key: FileKey, repo1: Repo, repo2: Repo) -> None:
        self.key = key
        super().__init__(f"File with annex key {key} exists in both {repo1.name} and {repo2.name} but has different contents.")

async def move_dataset(dataset: Dataset, from_repo: Repo, to_repo: Repo, where: List[TableFilter], limit: Optional[int] = None, moved_files: Optional[List[FileKey]] = None, ignore_missing = False) -> List[FileKey]:
    if moved_files is None:
        moved_files = []
    # TODO: Separate the concept of a Dolt remote from a Dolt-annex remote.
    # There may not be A Dolt remote to pull from
    # dataset.pull_from(remote_repo)
    async with dataset.with_repo(to_repo.uuid) as dest_repo_dataset:
        for to_table in dest_repo_dataset.get_tables():
            async with SyncOperation.context_manager(
                to_table = to_table,
                from_repo=from_repo,
                to_repo=to_repo,
                ignore_missing=ignore_missing,
            ) as sync_op:
                await sync_op.move(where, limit)
    # TODO: This only returns the files moved in the last table.
    return sync_op.files_moved

