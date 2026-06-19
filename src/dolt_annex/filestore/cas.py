import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing_extensions import Optional

from dolt_annex.datatypes.async_types import maybe_await, ReadableStream, AsyncContextManager
from dolt_annex.datatypes.async_utils import Result
from dolt_annex.datatypes.file_io import Path, async_bytes_io
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.base import FileKey, FileKeyGeneratingReader, FileKeyGenerator
from dolt_annex.filestore.base import FileStore

class ContentAddressableStorageError(Exception):
    pass

class ContentAddressableStorageKeyMismatchError(ContentAddressableStorageError):
    """Raised when a provided FileKey does not match the computed FileKey of the data being uploaded."""
    pass

@dataclass
class ContentAddressableStorage:
    file_store: FileStore
    file_key_format: FileKeyType
    alternate_key_formats: list[FileKeyType]

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
            file_key = await self.file_key_format.from_file(file_path)
        result = await maybe_await(self.put_file_object(file_path.open(), file_key))
        await result.wait_for_complete()
        return file_key

    async def copy_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the remote. If the repo is local, this must copy the file.
        
        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = await self.file_key_format.from_file(file_path)
        result = await maybe_await(self.put_file_object(file_path.open(), file_key=file_key))
        await result.wait_for_complete()
        return file_key

    async def put_file_bytes(self, file_bytes: bytes, file_key: Optional[FileKey] = None) -> Result[FileKey]:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = self.file_key_format.from_bytes(file_bytes)
        result = await maybe_await(self.file_store.put_file_object(async_bytes_io(file_bytes), file_key=file_key))
        return result.map(lambda _: file_key)
    
    def file_key_generators(self, extension: Optional[str] = None) -> list[FileKeyGenerator]:
        """Get a list of FileKeyGenerators for all supported key formats."""
        return [
            format.generator(extension=extension) for format in [self.file_key_format, *self.alternate_key_formats]
        ]
    
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey) -> Result[None]:
        """Upload a file-like object to the remote. If file_key is provided, it will be compared to the computed key and an error will be raised if they do not match."""

        # We only create key generators if the data source stream is opened.
        # This means that if the write is a no-op because it already exists in
        # the destination, we don't compute alias keys.
        generators = []
        @asynccontextmanager
        async def open_data_source() -> AsyncGenerator[ReadableStream]:
            nonlocal generators
            generators = self.file_key_generators(file_key.extension)
            async with data_source as in_fd:
                yield FileKeyGeneratingReader(in_fd, generators)

        result = await maybe_await(self.file_store.put_file_object(open_data_source(), file_key=file_key))
        await result.wait_for_complete()

        async def create_key_aliases():
            computed_keys = [generator.finalize() for generator in generators]
            if computed_keys and file_key is not None and not any(computed_key.same_bytes(file_key) for computed_key in computed_keys):
                raise ContentAddressableStorageKeyMismatchError(f"FileKey mismatch: provided key {file_key} does not match computed keys {computed_keys}")

            async with asyncio.TaskGroup() as tg:
                for alias in computed_keys:
                    if alias == file_key:
                        continue
                    result = await self.file_store.create_alias(old_key=file_key, new_key=alias)
                    tg.create_task(result.wait_for_complete())
        return result.and_then(create_key_aliases)
    
    async def verify_file(self, file_key: FileKey) -> None:
        """
        Assert that a file has the correct bytes by recomputing its key.
        """
        # TODO: Files that end in a . currently don't verify correctly, but they're rare in practice.
        if str(file_key)[-1] == '.':
            return
        async with self.file_store.with_file_object(file_key) as in_fd:
            actual_key = await type(file_key).from_fo(in_fd, extension=file_key.extension)
            assert actual_key == file_key, f"File key mismatch: {str(file_key)} was recomputed as {str(actual_key)}"

    async def verify_all_files(self) -> None:
        """
        Verify that all files in the filestore have the correct bytes by recomputing their keys.
        """
        async for file_key, in_fd in self.file_store.get_files():
            async with in_fd as in_fd_opened:
                actual_key = await type(file_key).from_fo(in_fd_opened, extension=file_key.extension)
                assert actual_key == file_key

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

async def filestore_copy(*, src: ContentAddressableStorage, dst: ContentAddressableStorage, key: FileKey) -> Result[None]:
    data_source = src.file_store.with_file_object(key)
    return await dst.put_file_object(data_source, file_key=key)
