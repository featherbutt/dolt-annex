#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing_extensions import Iterable, Optional, Tuple, List

from dolt_annex.datatypes import TableRow
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.async_utils import Result
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import FileTableSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.cas import filestore_copy
from dolt_annex.table import Dataset, FileTable, TableFilter
from dolt_annex.logger import logger
from dolt_annex.dolt import DoltSqlServer


class SyncOperation:
    table: FileTable
    from_repo: Repo
    to_repo: Repo
    ignore_missing: bool = False

    work_queue: asyncio.Queue[Optional[Tuple[FileKey, TableRow]]]
    files_moved: List[FileKey]
    pending_exceptions: List[Exception]

    def __init__(
            self,
            *,
            table: FileTable,
            from_repo: Repo,
            to_repo: Repo,
            ignore_missing: bool = False,
            queue_size: Optional[int] = 1000,
    ) -> None:
        self.table = table
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
            except FileNotFoundError as e:
                self.pending_exceptions.append(e)
            finally:
                self.work_queue.task_done()

    async def move(self, where: List[TableFilter], batch_size: Optional[int] = None) -> None:
        dolt = self.table.dolt

        has_more = True
        while has_more:
            keys_and_submissions = list(diff_keys(dolt, str(self.from_repo.uuid), str(self.to_repo.uuid), self.table.dataset_name, self.table.schema, where, batch_size))
            has_more = await self.move_submissions_and_keys(keys_and_submissions)
            # Await here so that the next diff sees the updated state
            await self.work_queue.join()
            if self.pending_exceptions:
                raise ExceptionGroup("exceptions during sync", self.pending_exceptions)
            await self.table.flush()

    async def move_submissions_and_keys(self, keys_and_submissions: Iterable[Tuple[str, FileKey, TableRow]]) -> bool:
        has_more = False
        for diff_type, key, table_row in keys_and_submissions:
            has_more = True
            await self.work_queue.put((key, table_row))
        return has_more
    
    async def move_submission_and_key(self, key: FileKey, table_row: TableRow) -> Result[None]:
        logger.info(f"moving {table_row}: {key}")

        if await maybe_await(self.to_repo.filestore.exists(key)):
            logger.debug(f"file {key} already exists in destination filestore")
            # The file may have come from a different dataset, so we don't need to copy it.
            # We still record that we have a copy of it for this dataset.
            await self.table.insert_file_source(table_row, key, self.to_repo.uuid)
            return Result.of(None)
        if self.ignore_missing and not await maybe_await(self.from_repo.filestore.exists(key)):
            logger.debug(f"Missing file {key} in source filestore, skipping due to --ignore-missing")
            return Result.of(None)
        result = await filestore_copy(src=self.from_repo.filestore, dst=self.to_repo.filestore, key=key)
        # We must wait for the copy to complete before updating the dataset.
        async def update_table_on_complete() -> None:
            await result.wait_for_complete()
            await self.table.insert_file_source(table_row, key, self.to_repo.uuid)
        return Result(asyncio.create_task(update_table_on_complete()))

    @classmethod
    @asynccontextmanager
    async def context_manager(
            cls,
            *,
            table: FileTable,
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
                table=table,
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
    for table in dataset.tables.values():
        async with SyncOperation.context_manager(
            table=table,
            from_repo=from_repo,
            to_repo=to_repo,
            ignore_missing=ignore_missing,
        ) as sync_op:
            await sync_op.move(where, limit)
    # TODO: This only returns the files moved in the last table.
    return sync_op.files_moved

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, dataset_name: str, file_key_table: FileTableSchema, filters: List[TableFilter], limit: Optional[int] = None) -> Iterable[Tuple[str, FileKey, TableRow]]:
    refs = [in_ref, not_in_ref]
    refs.sort()
    union_branch_name = f"union-{refs[0]}-{refs[1]}-{dataset_name}"
    
    in_ref_branch = f"{in_ref}-{dataset_name}"
    not_in_ref_branch = f"{not_in_ref}-{dataset_name}"
    # Create the union branch if it doesn't exist
    # What if in_ref_branch hasn't been created yet? We need an approach that abstracts this away.
    # Don't pass branch names around as strings, pass them as first class objects.
    with dolt.maybe_create_branch(union_branch_name, in_ref_branch):
        dolt.merge(in_ref_branch)
        dolt.merge(not_in_ref_branch)
        query = diff_query(file_key_table, filters)
        if limit is not None:
            query += " LIMIT %s"
            query_results = dolt.query(query, (not_in_ref_branch, union_branch_name, limit))
        else:
            query_results = dolt.query(query, (not_in_ref_branch, union_branch_name))
        # TODO: Wrap this in a helper function
        for (annex_key, diff_type, *key_parts) in query_results:
            yield (diff_type, FileKey.must_parse(bytes(annex_key, encoding='utf-8')), TableRow(tuple(key_parts)))

def diff_query(file_key_table: FileTableSchema, filters: List[TableFilter]) -> str:
    """
    Generates a SQL query to identify the files that exist on one remote but not another.
    Note that generating a SQL query this way is not safe from SQL injection, but SQL injection
    isn't part of the threat model, since any query that the application can run,
    the user can already run themselves.
    """
    return f"""
        SELECT
            to_{file_key_table.file_column}, `diff_type`, {",".join("to_" + col for col in file_key_table.key_columns)}
        FROM dolt_commit_diff_{file_key_table.name}
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s)
        {''.join(f" AND to_{f.column_name} = %s" for f in filters)}
        """

