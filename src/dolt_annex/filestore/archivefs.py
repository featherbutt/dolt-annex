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
from typing import AsyncGenerator, Generator, Optional
import uuid
from filelock import FileLock, Timeout
from pydantic import InstanceOf
from typing_extensions import override, Tuple

from fs.base import FS as FileSystem
import fs.osfs

from dolt_annex.datatypes.async_types import MaybeAwaitable, maybe_await, AsyncContextManager, ReadableStream
from dolt_annex.datatypes.async_utils import Result, await_or_enter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import Path, async_open
from dolt_annex.file_keys import FileKey
from dolt_annex.filestore.file_handles import ExistingFileHandle
from dolt_annex.tarfile_utils import addfile, advance_to_end

from .base import FileInfo, FileStore, FileStoreModel

class ArchiveFS(FileStore):
    """
    ArchiveFS is a filestore that stores many files in a few archive files.
    
    It relies on a secondary filestore to map file keys to archive files and offsets within those files.

    This reduces the size of values in the secondary filestore.
    """
    
    file_system: FileSystem
    secondary: FileStore
    files_queue: asyncio.Queue[Tuple[FileKey, AsyncContextManager[ReadableStream], asyncio.Future[None]]]
    workers: asyncio.TaskGroup
    max_archive_size: int

    writable_archives_dir: Path
    finalized_archives_dir: Path
    locks_dir: Path

    def __init__(
            self, *,
            file_system: FileSystem,
            secondary: FileStore,
            workers: asyncio.TaskGroup,
            num_workers: int,
            max_archive_size: int,
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

        self.files_queue = asyncio.Queue()
        for _ in range(num_workers):
            workers.create_task(self._worker_loop())

    @contextmanager
    def open_archive_file_for_write(self, archive_file_path: Path) -> Generator[Optional[tarfile.TarFile]]:
        lock_path = self.locks_dir / f"{archive_file_path.name}.lock"
        lock_syspath = lock_path.getsyspath()
        assert lock_syspath is not None, "ArchiveFS requires a local filesystem that supports file locks."
        try:
            lock = FileLock(lock_syspath, blocking=False)
            with lock:
                archive_file_path.touch()
                archive_fd_sync = archive_file_path.open_sync('r+b')
                archive_tar = tarfile.open(fileobj=archive_fd_sync, mode='w')
                yield archive_tar
        except Timeout:
            # The lock is held by another process, so we cannot access this archive file.
            yield None

    @contextmanager
    def get_archive_file_for_write(self) -> Generator[Tuple[tarfile.TarFile, Path], None, None]:
        # Attempt to acquire a lock on a "hot" archive file.
        for archive_file in self.writable_archives_dir.children():
            with self.open_archive_file_for_write(archive_file) as archive_tar:
                if archive_tar is not None:
                    yield archive_tar, archive_file
                    return
        
        # If we cannot acquire a lock on any existing writable archive files, create a new one.
        while True:
            new_archive_file_name = f"{uuid.uuid7()}.tar"
            new_archive_file_path = self.writable_archives_dir / new_archive_file_name
            with self.open_archive_file_for_write(new_archive_file_path) as archive_tar:
                if archive_tar is not None:
                    yield archive_tar, new_archive_file_path
                    return

    async def _worker_loop(self) -> None:
        while True:
            with self.get_archive_file_for_write() as (archive_tar, archive_fd_sync, archive_file):
                async with async_open(archive_fd_sync) as archive_fd:
                    with archive_tar:
                        advance_to_end(archive_tar)
                        while archive_tar.offset < self.max_archive_size:
                            try:
                                file_key, data_source, callback = await self.files_queue.get()
                            except asyncio.QueueShutDown:
                                return
                            try:
                                tar_info = tarfile.TarInfo(name=str(file_key))
                                tar_info.size = file_key.size
                                buf = tar_info.tobuf(archive_tar.format, archive_tar.encoding, archive_tar.errors)
                                offset = archive_tar.offset + len(buf)
                                async with data_source as in_fd:
                                    await addfile(archive_tar, tarfile_fd=archive_fd, tarinfo=tar_info, input_fileobj=in_fd)
                                # TODO: We probably don't need to flush immediately after each write,
                                # But there's no way to signal a flush for tests.
                                archive_fd_sync.flush()
                                secondary_value = f"{archive_file.name}:{offset}:{tar_info.size}"
                                await maybe_await(self.secondary.put_file_bytes(secondary_value.encode('utf-8'), file_key))
                                callback.set_result(None)
                            except Exception as e:
                                callback.set_exception(e)
                            finally:
                                self.files_queue.task_done()

            # Move the archive file to the finalized directory so that it is no longer used for writing.
            archive_file.rename(self.finalized_archives_dir / archive_file.name)

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey) -> Result[None]:
        if await maybe_await(self.secondary.exists(file_key)):
            # The file already exists, so we don't need to do anything.
            # Open the stream in order to close it.
            async with data_source:
                return Result.done()
        callback = asyncio.Future[None]()
        await self.files_queue.put((file_key, data_source, callback))
        return Result(callback)

    @override
    @await_or_enter
    async def get_file_object(self, file_key: FileKey) -> AsyncGenerator[ExistingFileHandle]:
        secondary_value = (await self.secondary.get_file_bytes(file_key)).decode('utf-8')
        archive_file_name, offset_str, size_str = secondary_value.split(':')
        offset = int(offset_str)
        size = int(size_str)

        # Try to find the archive file in writable_archives_dir first, then finalized_archives_dir
        archive_file_path = self.writable_archives_dir / archive_file_name
        if not archive_file_path.exists():
            archive_file_path = self.finalized_archives_dir / archive_file_name

        archive_fd = archive_file_path.open_sync('rb')
        file_in_file = tarfile._FileInFile(archive_fd, offset, size, str(file_key), blockinfo=None)
        yield ExistingFileHandle(await async_open(file_in_file), FileInfo(size=size))

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        async with self.get_file_object(file_key) as file_obj:
            return file_obj.file_info

    @override
    def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, ExistingFileHandle):
            raise TypeError("ArchiveFS.fstat was passed a file object that did not originate from this filestore.")

        return file_obj.file_info

    @override
    def exists(self, file_key: FileKey) -> MaybeAwaitable[bool]:
        return self.secondary.exists(file_key)
    
    @override
    async def flush(self) -> None:
        await self.files_queue.join()
        await maybe_await(self.secondary.flush())

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> Result[None]:
        return await self.secondary.create_alias(old_key, new_key)

class ArchiveFSModel(FileStoreModel):
    root: pathlib.Path | InstanceOf[FileSystem]
    secondary: FileStoreModel

    # The number of parallel workers to use for writing archives.
    # Each worker has an exclusive lock on a different archive file.
    num_workers: int = 4

    # The maximum size of each archive file, in bytes.
    # If an archive file would exceed this size, a new archive file will be created.
    max_archive_size: int = 4 * (2 << 30)  # 4 GiB

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
                max_archive_size=self.max_archive_size
            )
            try:
                yield archive
            finally:
                archive.files_queue.shutdown()
            
    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"ArchiveFS({self.secondary.type_name()})"


