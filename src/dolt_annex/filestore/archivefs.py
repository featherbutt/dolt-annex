#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import pathlib
import tarfile
from typing import AsyncGenerator, Awaitable, List, NamedTuple
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
from dolt_annex.tarfile_utils import TarFile

from .base import FileInfo, FileStore, FileStoreError, FileStoreModel

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
    
    # A list of archived files that are open and available for writing.
    # put_file_object pops files before writing, and pushes them back after writing.
    available_archives: List[TarFile]
    exit_stack: AsyncExitStack
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
            exit_stack: AsyncExitStack,
            max_archive_size: int,
            append: bool,
            readonly: bool,
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
        self.available_archives = []
        self.exit_stack = exit_stack

        self.append = append
        self.readonly = readonly

    @asynccontextmanager
    async def get_archive_file_for_write(self) -> AsyncGenerator[TarFile, None]:
        # Attempt to acquire a lock on a "hot" archive file.
        if self.append:
            for archive_file in self.writable_archives_dir.children():
                try:
                    async with TarFile.new(self.lock_manager, archive_file) as archive:
                        yield archive
                        return
                except FailedToAcquireLock:
                    continue
        
        # If we cannot acquire a lock on any existing writable archive files, create a new one.
        while True:
            new_archive_file_name = f"{uuid.uuid4()}.tar"
            new_archive_file_path = self.writable_archives_dir / new_archive_file_name
            try:
                async with TarFile.new(self.lock_manager, new_archive_file_path) as archive:
                    yield archive
                    return
            except FailedToAcquireLock:
                continue

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key_producer: Awaitable[FileKey], overwrite_existing: bool = False) -> FileKey:
        if self.readonly:
            raise FileStoreError("Cannot write to a readonly ArchiveFS filestore.")
        # get an open achive file if possible, else create a new one.
        if self.available_archives:
            tarfile = self.available_archives.pop()
        else:
            tarfile = await self.exit_stack.enter_async_context(self.get_archive_file_for_write())
        async with data_source as in_fd:
            success, file_key, offset, file_size = await tarfile.addfile(input_fileobj=in_fd, file_key_producer=file_key_producer, exists=self.exists, overwrite_existing=overwrite_existing)
            if not success:
                self.available_archives.append(tarfile)
                return file_key
            
            # TODO: We probably don't need to flush immediately after each write,
            # But there's no way to signal a flush for tests.

            tarfile.fd_sync.flush()
            secondary_value = f"{tarfile.path.name}:{offset}:{file_size}"
            await self.secondary.put_file_bytes(secondary_value.encode('utf-8'), file_key)

        self.available_archives.append(tarfile)
        return file_key

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

    readonly: bool = False

    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[ArchiveFS]:
        async with (
            self.secondary.open(config) as secondary_filestore,
            asyncio.TaskGroup() as workers,
            AsyncExitStack() as exit_stack
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
                exit_stack=exit_stack,
                max_archive_size=self.max_archive_size,
                append=self.append,
                readonly=self.readonly
            )
            yield archive
            
    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"ArchiveFS({self.secondary.type_name()})"
