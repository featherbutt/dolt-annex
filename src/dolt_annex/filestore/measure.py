#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
from typing_extensions import override

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, MaybeAwaitable, ReadableFileObject, ReadableStream
from dolt_annex.datatypes.async_utils import Result
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import FileInfo
from dolt_annex.file_keys import FileKey
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.base import FileStoreModel

@dataclass
class Measure(FileStore):
    """
    A Measure FileStore wraps a child file store and tracks additional metrics,
    such as the total file size and the number of files stored.

    If using batching, the metrics are only flushed when the batch is flushed.
    If the underlying filestore does not support batching, then an unexpected
    termination may cause some operations to not be reflected in the metrics.
    This is acceptable, and metrics should be seen as approximate.
    """
    
    child: FileStore

    stats_file: TextIOWrapper

    file_count: int
    total_file_size: int

    @override
    def flush(self) -> None:
        """Flush the current stats information to disk."""
        self.stats_file.seek(0)
        self.stats_file.truncate()
        self.stats_file.write(f"{self.file_count},{self.total_file_size}")
        self.stats_file.flush()

    @override
    def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey) -> MaybeAwaitable[Result[None]]:
        """Upload a file-like object to the remote. If file_key is not provided, it will be computed."""
        return self.child.put_file_object(data_source, file_key)

    @override
    def get_file_object(self, file_key: FileKey) -> AwaitOrEnter[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""
        return self.child.get_file_object(file_key)

    @override
    def exists(self, file_key: FileKey) -> MaybeAwaitable[bool]:
        """
        Returns whether the key exists in the filestore.
        """
        return self.child.exists(file_key)

    @override
    def stat(self, file_key: FileKey) -> MaybeAwaitable[FileInfo]:
         return self.child.stat(file_key)

    @override
    def fstat(self, file_obj: ReadableStream) -> MaybeAwaitable[FileInfo]:
         return self.child.fstat(file_obj)

class MeasureModel(FileStoreModel):
    child: FileStoreModel

    stats_file_path: Path

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[FileStore]:
        """Open the filestore, loading or initializing metrics tracking."""

        with open(self.stats_file_path, 'r+', encoding='utf-8') as stats_file:
            stats = stats_file.read().split(',')
            if len(stats) == 2:
                file_count = int(stats[0])
                total_file_size = int(stats[1])
            else:
                file_count = 0
                total_file_size = 0

            async with self.child.open(config) as child:
                filestore = Measure(
                    child=child,
                    stats_file=stats_file,
                    file_count=file_count,
                    total_file_size=total_file_size,
                )
                yield filestore
                filestore.flush()