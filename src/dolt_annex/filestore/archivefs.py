#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LevelDB is a filestore type that stores every file in a LevelDB key-value store,
with the file key as the key and the file contents as the value.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import pathlib
import tarfile
from typing import AsyncGenerator
from pydantic import InstanceOf
from typing_extensions import override, Tuple

from fs.base import FS as FileSystem
import fs.osfs

from dolt_annex.datatypes.async_utils import MaybeAwaitable, maybe_await
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import FileObject, Path, ReadableFileObject
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, FileStoreModel

class ArchiveFS(FileStore):
    """
    ArchiveFS is a filestore that stores many files in a few archive files.
    
    It relies on a secondary filestore to map file keys to archive files and offsets within those files.

    This reduces the size of values in the secondary filestore.
    """
    
    file_system: FileSystem
    secondary: FileStore
    files_queue: asyncio.Queue[Tuple[FileKey, ReadableFileObject]]

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
        # Iterate over the current "hot" archive files, assigning each one to a worker.
        self.files_queue = asyncio.Queue()
        for i in range(num_workers):
            archive_file = Path(file_system) / f"archive_{i}.tar"
            # TODO: Rotate archive files if they exceed max_archive_size.
            workers.create_task(self._worker_loop(archive_file))

    async def _worker_loop(self, archive_file: Path) -> None:
        if not archive_file.exists() or archive_file.stat().size == 0:
            archive_fd = archive_file.open('wb')
            archive_tar = tarfile.open(fileobj=archive_fd, mode='w')
        else:
            archive_fd = archive_file.open('r+b')
            archive_tar = tarfile.open(fileobj=archive_fd, mode='a')
        with archive_fd, archive_tar:
            while True:
                try:
                    file_key, in_fd = await self.files_queue.get()
                except asyncio.QueueShutDown:
                    break
                try:
                    tar_info = tarfile.TarInfo(name=str(file_key))
                    tar_info.size = file_key.size()
                    buf = tar_info.tobuf(archive_tar.format, archive_tar.encoding, archive_tar.errors)
                    offset = archive_tar.offset + len(buf)
                    # TODO: Rotate archive files if they exceed max_archive_size.
                    archive_tar.addfile(tarinfo=tar_info, fileobj=in_fd)
                    secondary_value = f"{archive_file.name}:{offset}:{tar_info.size}"
                    await maybe_await(self.secondary.put_file_bytes(secondary_value.encode('utf-8'), file_key))
                finally:
                    await maybe_await(in_fd.close())
                    self.files_queue.task_done()



    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        await self.files_queue.put((file_key, in_fd))
        await self.files_queue.join()

    @override
    async def get_file_object(self, file_key: FileKey) -> FileObject:
        secondary_value = (await self.secondary.get_file_bytes(file_key)).decode('utf-8')
        archive_file_name, offset_str, size_str = secondary_value.split(':')
        offset = int(offset_str)
        size = int(size_str)

        archive_file_path = Path(self.file_system) / archive_file_name
        archive_fd = archive_file_path.open('rb')
        return tarfile._FileInFile(archive_fd, offset, size, str(file_key), blockinfo=None)

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        file_obj = await self.get_file_object(file_key)
        return FileInfo(size=file_obj.size)

    @override
    def fstat(self, file_obj: ReadableFileObject) -> FileInfo:
        return FileInfo(size=file_obj.size)

    @override
    def exists(self, file_key: FileKey) -> MaybeAwaitable[bool]:
        return self.secondary.exists(file_key)

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
            yield archive
            archive.files_queue.shutdown()
            