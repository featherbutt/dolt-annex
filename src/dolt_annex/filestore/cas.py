from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from io import BytesIO
from typing_extensions import Optional, AsyncContextManager

from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.file_io import Path, ReadableFileObject
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStore

@dataclass
class ContentAddressableStorage:
    file_store: FileStore
    file_key_format: FileKeyType

    _batch_size: Optional[int] = None
    _pending_changes: int = 0

    async def tick(self) -> None:
        """Record a single operation in the current batch, then possibly flush."""
        if self._batch_size is not None:
            self._pending_changes += 1
            if self._pending_changes >= self._batch_size:
                await maybe_await(self.file_store.flush())
                self._pending_changes = 0

    async def put_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the repo. If the repo is local, this is allowed to move the file.
        
        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_file(file_path)
        await maybe_await(self.file_store.put_file(file_path, file_key))
        return file_key

    async def copy_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the remote. If the repo is local, this must copy the file.
        
        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_file(file_path)
        with open(file_path, 'rb') as fd:
            await maybe_await(self.file_store.put_file_object(fd, file_key=file_key))
        return file_key

    async def put_file_bytes(self, file_bytes: bytes, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_bytes(file_bytes)
        fd = BytesIO(file_bytes)
        fd.name = str(file_key)
        await maybe_await(self.file_store.put_file_object(fd, file_key=file_key))
        return file_key

    async def put_file_object(self, in_fd: ReadableFileObject, file_key: Optional[FileKey] = None) -> FileKey:
        """Upload a file-like object to the remote. If file_key is not provided, it will be computed."""
        if file_key is None:
            file_key = self.file_key_format.from_fo(in_fd)
        await maybe_await(self.file_store.put_file_object(in_fd, file_key=file_key))
        return file_key

    async def batch(self, batch_size: Optional[int]=10000) -> AsyncContextManager[None]:
        """
        Some file stores are more efficient when performing multiple operations in a batch.

        This comes at the cost of atomicity: if the process terminates unexpectedly during a batch,
        some or all of the operations in the batch may not be completed. However, the filestore
        should remain in a consistent state regardless.

        If batch_size is provided, then the filestore will flush the batch after
        that many operations have been performed.
        """

        @asynccontextmanager
        async def batch() -> AsyncGenerator[None]:
            original_batch_size = self._batch_size
            self._batch_size = batch_size
            yield
            await maybe_await(self.file_store.flush())
            self._pending_changes = 0
            self._batch_size = original_batch_size

        return batch()
