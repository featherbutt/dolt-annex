#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import BinaryIO
from typing_extensions import Optional, ContextManager, Generator

from dolt_annex.datatypes.common import YesNoMaybe
from dolt_annex.datatypes.pydantic import AbstractBaseModel
from dolt_annex.file_keys import FileKey, FileKeyType
class FileStore(AbstractBaseModel):

    def put_file(self, file_path: Path, file_key: FileKey) -> None:
        """
        Upload an on-disk file to the repo. If the repo is local, this is allowed to move the file.
        """
        return self.put_file(file_path, file_key)

    def copy_file(self, file_path: Path, file_key: FileKey) -> None:
        """
        Upload an on-disk file to the remote. If the repo is local, this must copy the file.
        """
        if file_key is None:
            file_key = self.file_key_format.from_file(file_path)
        with open(file_path, 'rb') as fd:
            self.put_file_object(fd, file_key)

    def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> None:
        """
        Upload an in-memory file to the remote.
        """
        self.put_file_object(BytesIO(file_bytes), file_key=file_key)

    @abstractmethod
    def put_file_object(self, in_fd: BinaryIO, file_key: FileKey) -> None:
        """Upload a file-like object to the remote."""

    @abstractmethod
    def get_file_object(self, file_key: FileKey) -> ContextManager[BinaryIO]:
        """Get a file-like object for a file in the remote by its key."""

    @abstractmethod
    def exists(self, file_key: FileKey) -> bool:
        """
        Returns whether the key exists in the filestore.
        """

    def possibly_exists(self, file_key: FileKey) -> YesNoMaybe:
        """
        If false, the file definitely does not exist in the filestore.
        This is often more efficient than calling exists.
        """
        if self.exists(file_key):
            return YesNoMaybe.YES
        return YesNoMaybe.NO

    def open(self, config: 'Config') -> ContextManager[None]:
        """
        Open the filestore for use. This may involve setting up connections, opening files, etc.

        Returns a context manager that yields the opened filestore instance.
        """
        @contextmanager
        def inner() -> Generator[None]:
            yield
            self.flush()

        return inner()

    def flush(self) -> None:
        """Flush any pending operations to the filestore."""


@dataclass
class ContentAddressableStorage:
    file_store: FileStore
    file_key_format: FileKeyType

    _batch_size: Optional[int] = None
    _pending_changes: int = 0

    def tick(self) -> None:
        """Record a single operation in the current batch, then possibly flush."""
        if self._batch_size is not None:
            self._pending_changes += 1
            if self._pending_changes >= self._batch_size:
                self.file_store.flush()
                self._pending_changes = 0

    def put_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the repo. If the repo is local, this is allowed to move the file.
        
        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_file(file_path)
        self.file_store.put_file(file_path, file_key)
        return file_key

    def copy_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the remote. If the repo is local, this must copy the file.
        
        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_file(file_path)
        with open(file_path, 'rb') as fd:
            self.file_store.put_file_object(fd, file_key)
        return file_key

    def put_file_bytes(self, file_bytes: bytes, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_bytes(file_bytes)
        self.file_store.put_file_object(BytesIO(file_bytes), file_key=file_key)
        return file_key

    def put_file_object(self, in_fd: BinaryIO, file_key: Optional[FileKey] = None) -> FileKey:
        """Upload a file-like object to the remote. If file_key is not provided, it will be computed."""
        if file_key is None:
            file_key = self.file_key_format.from_fo(in_fd)
        self.file_store.put_file_object(in_fd, file_key=file_key)
        return file_key

    def batch(self, batch_size: Optional[int]=10000) -> ContextManager[None]:
        """
        Some file stores are more efficient when performing multiple operations in a batch.

        This comes at the cost of atomicity: if the process terminates unexpectedly during a batch,
        some or all of the operations in the batch may not be completed. However, the filestore
        should remain in a consistent state regardless.

        If batch_size is provided, then the filestore will flush the batch after
        that many operations have been performed.
        """

        @contextmanager
        def batch() -> Generator[None]:
            original_batch_size = self._batch_size
            self._batch_size = batch_size
            yield
            self.file_store.flush()
            self._pending_changes = 0
            self._batch_size = original_batch_size

        return batch()


def copy(*, src: BinaryIO, dst: BinaryIO, buffer_size=4096):
    while True:
        buf = src.read(buffer_size)
        if not buf:
            break
        dst.write(buf)
