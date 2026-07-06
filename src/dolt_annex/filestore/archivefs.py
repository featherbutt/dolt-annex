#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
import pathlib
import tarfile
from typing import AsyncGenerator, Awaitable, BinaryIO, Generator, NamedTuple
import uuid
from pydantic import InstanceOf, SerializeAsAny
from typing_extensions import override, Tuple
import logging

from fs.base import FS as FileSystem
import fs.osfs

from dolt_annex.datatypes.async_types import AwaitOrEnter, maybe_await, AsyncContextManager, ReadableStream
from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import Path, async_open
from dolt_annex.datatypes.locking import FailedToAcquireLock, LockManager, new_lock_manager
from dolt_annex.file_keys import FileKey
from dolt_annex.filestore.file_handles import ExistingFileHandle
from dolt_annex.tarfile_utils import addfile, advance_to_end

from .base import FileInfo, FileStore, FileStoreModel

logger = logging.getLogger(__name__)

class ArchiveFS(FileStore):
    """
    ArchiveFS is a filestore that stores many files in a few archive files.

    It relies on a secondary filestore to map file keys to archive files and offsets within those files.

    This reduces the size of values in the secondary filestore.
    """

    class QueueItem(NamedTuple):
        file_key_producer: Awaitable[FileKey]
        data_source: AsyncContextManager[ReadableStream]
        callback: asyncio.Future[FileKey]
        overwrite_existing: bool
        
    file_system: FileSystem
    secondary: FileStore
    files_queue: asyncio.Queue[QueueItem]
    workers: asyncio.TaskGroup
    max_archive_size: int
    append: bool

    writable_archives_dir: Path
    finalized_archives_dir: Path
    locks_dir: Path
    lock_manager: LockManager

    def __init__(
            self, *,
            file_system: FileSystem,
            secondary: FileStore,
            workers: asyncio.TaskGroup,
            num_workers: int,
            max_archive_size: int,
            append: bool
    ):
        self.file_system = file_system
        self.secondary = secondary
        self.workers = workers
        self.max_archive_size = max_archive_size

        self.writable_archives_dir = Path(self.file_system, "writable_archives")
        self.finalized_archives_dir = Path(self.file_system, "finalized_archives")
        self.locks_dir = Path(self.file_system, "locks")

        self.writable_archives_dir.mkdirs(exist_ok=True)
        self.finalized_archives_dir.mkdirs(exist_ok=True)
        self.locks_dir.mkdirs(exist_ok=True)

        self.lock_manager = new_lock_manager(self.locks_dir)
        self.files_queue = asyncio.Queue()

        self.append = append

        for _ in range(num_workers):
            workers.create_task(self._worker_loop())

    @contextmanager
    def open_archive_file_for_write(self, archive_file_path: Path) -> Generator[Tuple[tarfile.TarFile, BinaryIO], None, None]:
        with self.lock_manager.lock(archive_file_path.name):
            archive_file_path.touch()
            archive_fd_sync = archive_file_path.open_sync('r+b')
            archive_tar = tarfile.open(fileobj=archive_fd_sync, mode='w')
            yield archive_tar, archive_fd_sync
            archive_tar.close()
            archive_fd_sync.close()

    @contextmanager
    def get_archive_file_for_write(self) -> Generator[Tuple[tarfile.TarFile, BinaryIO, Path], None, None]:
        # Attempt to acquire a lock on a "hot" archive file.
        if self.append:
            for archive_file in self.writable_archives_dir.children():
                try:
                    with self.open_archive_file_for_write(archive_file) as (archive_tar, archive_fd):
                        yield archive_tar, archive_fd, archive_file
                        return
                except FailedToAcquireLock:
                    continue
        
        # If we cannot acquire a lock on any existing writable archive files, create a new one.
        while True:
            new_archive_file_name = f"{uuid.uuid4()}.tar"
            new_archive_file_path = self.writable_archives_dir / new_archive_file_name
            try:
                with self.open_archive_file_for_write(new_archive_file_path) as (archive_tar, archive_fd):
                    yield archive_tar, archive_fd, new_archive_file_path
                    return
            except FailedToAcquireLock:
                continue

    async def _worker_loop(self) -> None:
        while True:
            with self.get_archive_file_for_write() as (archive_tar, archive_fd_sync, archive_file):
                async with async_open(archive_fd_sync) as archive_fd:
                    with archive_tar:
                        advance_to_end(archive_tar)
                        while archive_tar.offset < self.max_archive_size:
                            try:
                                file_key_producer, data_source, callback, overwrite_existing = await self.files_queue.get()
                            except asyncio.QueueShutDown:
                                return
                            try:
                                async with data_source as in_fd:
                                    success, file_key, offset, file_size = await addfile(archive_tar, tarfile_fd=archive_fd, input_fileobj=in_fd, file_key_producer=file_key_producer, exists=self.exists, overwrite_existing=overwrite_existing)
                                    if not success:
                                        callback.set_result(file_key)
                                        continue
                                # TODO: We probably don't need to flush immediately after each write,
                                # But there's no way to signal a flush for tests.

                                archive_fd_sync.flush()
                                secondary_value = f"{archive_file.name}:{offset}:{file_size}"
                                await maybe_await(self.secondary.put_file_bytes(secondary_value.encode('utf-8'), file_key))
                                callback.set_result(file_key)
                            except Exception as e:
                                callback.set_exception(e)
                            finally:
                                self.files_queue.task_done()

            # Move the archive file to the finalized directory so that it is no longer used for writing.
            archive_file.rename(self.finalized_archives_dir / archive_file.name)

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key_producer: Awaitable[FileKey], overwrite_existing: bool = False) -> FileKey:
        callback = asyncio.Future[FileKey]()
        await self.files_queue.put(ArchiveFS.QueueItem(file_key_producer, data_source, callback, overwrite_existing))
        return await callback

    @await_or_enter
    async def decode_secondary_value(self, file_key: FileKey, secondary_value_bytes: bytes) -> AsyncGenerator[ExistingFileHandle]:
        secondary_value = secondary_value_bytes.decode('utf-8')
        archive_file_name, offset_str, size_str = secondary_value.split(':')
        offset = int(offset_str)
        size = int(size_str)

        # Try to find the archive file in writable_archives_dir first, then finalized_archives_dir
        archive_file_path = self.writable_archives_dir / archive_file_name
        if not archive_file_path.exists():
            archive_file_path = self.finalized_archives_dir / archive_file_name

        archive_fd = archive_file_path.open_sync('rb')
        file_in_file = tarfile._FileInFile(archive_fd, offset, size, str(file_key), blockinfo=None)
        fd = await async_open(file_in_file)
        yield ExistingFileHandle(fd, FileInfo(size=size))
        await fd.close()
        archive_fd.close()
    
    @override
    @await_or_enter
    async def get_file_object(self, file_key: FileKey) -> AsyncGenerator[ExistingFileHandle]:
        file_bytes = await self.secondary.get_file_bytes(file_key)
        async with self.decode_secondary_value(file_key, file_bytes) as fd:
            yield fd

    @override
    async def get_files(self, prefix: bytes = b"") -> AsyncGenerator[Tuple[FileKey, AwaitOrEnter[ReadableStream]]]:
        async for key, value in self.secondary.get_files(prefix):
            async with value as value_opened:
                value_bytes = await value_opened.read()
                fd = self.decode_secondary_value(key, value_bytes)
                yield key, fd

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        async with self.with_file_object(file_key) as file_obj:
            return file_obj.file_info

    @override
    def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, ExistingFileHandle):
            raise TypeError("ArchiveFS.fstat was passed a file object that did not originate from this filestore.")

        return file_obj.file_info

    @override
    async def exists(self, file_key: FileKey) -> bool:
        return await maybe_await(self.secondary.exists(file_key))

    @override
    async def flush(self) -> None:
        await self.files_queue.join()
        await maybe_await(self.secondary.flush())

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        return await self.secondary.create_alias(old_key, new_key)

    @override
    def delete(self, key: FileKey) -> None:
        """
        Remove a file from a filestore. Note that this causes the filestore to "forget" about the file, but does not delete the file from the archive files.
        """
        self.secondary.delete(key)

class ArchiveFSModel(FileStoreModel):
    root: pathlib.Path | InstanceOf[FileSystem]
    secondary: SerializeAsAny[FileStoreModel]

    # The number of parallel workers to use for writing archives.
    # Each worker has an exclusive lock on a different archive file.
    num_workers: int = 4

    # The maximum size of each archive file, in bytes.
    # If an archive file would exceed this size, a new archive file will be created.
    max_archive_size: int = 8 * (1 << 30)  # 8 GiB

    append: bool = False

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[ArchiveFS]:
        async with (
            self.secondary.open(config) as secondary_filestore,
            asyncio.TaskGroup() as workers
        ):
            if isinstance(self.root, pathlib.Path):
                self.root.mkdir(parents=True, exist_ok=True)
                file_system = fs.osfs.OSFS(str(self.root))
            else:
                file_system = self.root
            
            archive = ArchiveFS(
                file_system=file_system,
                secondary=secondary_filestore,
                workers=workers,
                num_workers=self.num_workers,
                max_archive_size=self.max_archive_size,
                append=self.append
            )
            try:
                yield archive
            finally:
                archive.files_queue.shutdown()
            
    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"ArchiveFS({self.secondary.type_name()})"
