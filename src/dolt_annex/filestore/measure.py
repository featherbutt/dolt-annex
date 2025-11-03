#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from io import TextIOWrapper
from pathlib import Path
from typing_extensions import Optional, ContextManager, Generator, override, Self, BinaryIO

from dolt_annex.datatypes.config import Config
from dolt_annex.file_keys import FileKey
from dolt_annex.filestore import FileStore

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

    stats_file_path: Path
    _stats_file: TextIOWrapper

    _file_count: int
    _total_file_size: int

    @override
    def flush(self) -> None:
        """Flush the current stats information to disk."""
        self._stats_file.seek(0)
        self._stats_file.truncate()
        self._stats_file.write(f"{self._file_count},{self._total_file_size}")
        self._stats_file.flush()

    @override
    def put_file_object(self, in_fd: BinaryIO, file_key: Optional[FileKey] = None) -> None:
        """Upload a file-like object to the remote. If file_key is not provided, it will be computed."""
        return self.child.put_file_object(in_fd, file_key)

    @override
    def get_file_object(self, file_key: FileKey) -> ContextManager[BinaryIO]:
        """Get a file-like object for a file in the remote by its key."""
        return self.child.get_file_object(file_key)

    @override
    def exists(self, file_key: FileKey) -> bool:
        """
        Returns whether the key exists in the filestore.
        """
        return self.child.exists(file_key)


    @override
    def open(self, config: Config) -> ContextManager[None]:
        """Open the filestore, loading or initializing metrics tracking."""
        @contextmanager
        def inner(self: Self) -> Generator[None]:
            with open(self.stats_file_path, 'r+', encoding='utf-8') as self._stats_file:
                stats = self._stats_file.read().split(',')
                if len(stats) == 2:
                    self._file_count = int(stats[0])
                    self._total_file_size = int(stats[1])
                else:
                    self._file_count = 0
                    self._total_file_size = 0

                with self.child.open(config):
                    yield

                self.flush()

        return inner(self)
