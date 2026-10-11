#!/usr/bin/env python
# -*- coding: utf-8 -*-``

from contextlib import asynccontextmanager
from dataclasses import dataclass
from tarfile import HeaderError, TarInfo, BLOCKSIZE, NUL
import tarfile
from typing import AsyncGenerator, Awaitable, BinaryIO, Callable, NamedTuple, Self

from dolt_annex.datatypes.async_types import ReadableStream, WritableFileObject
from dolt_annex.datatypes.file_io import Path, async_open
from dolt_annex.datatypes.locking import LockManager
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import copy

def advance_to_end(tarfile: tarfile.TarFile):
    """
    The standard library tarfile module raises an exception and closes if
    the file does not end with two empty blocks after the last member.

    We want to be more permissive, which means manually advancing to the end of the archive file.
    If we encounter an unexpected EOF or if the file ends with a non-empty block,
    we simply seek to the end of the last member and continue writing from there.
    """
    while True:
        tarfile.fileobj.seek(tarfile.offset)
        previous_offset = tarfile.offset
        try:
            tarinfo = tarfile.tarinfo.fromtarfile(tarfile)
            # Calling fromtarfile advances the offset to the start of the next block.
            # Check that the file is not truncated by attempting to read the last byte of the previous block.
            tarfile.fileobj.seek(tarfile.offset - 1)
            if tarfile.fileobj.read(1) == b'':
                tarfile.fileobj.seek(previous_offset)
                break
            tarfile.members.append(tarinfo)
        except HeaderError:
            tarfile.fileobj.seek(tarfile.offset)
            break

class TarFileEntry(NamedTuple):
    success: bool
    file_key: FileKey
    offset: int
    size: int

@dataclass
class TarFile:
    path: Path
    tarfile: tarfile.TarFile
    fd: WritableFileObject
    fd_sync: BinaryIO

    _closed: bool = False

    @classmethod
    @asynccontextmanager
    async def new(cls, lock_manager: LockManager, archive_file_path: Path) -> AsyncGenerator[Self, None]:
        with lock_manager.lock(archive_file_path.name):
            archive_file_path.touch()
            archive_fd_sync = archive_file_path.open_sync('r+b')
            async with async_open(archive_fd_sync) as archive_fd:  
                with tarfile.open(fileobj=archive_fd_sync, mode='w') as archive_tar:
                    self = cls(path=archive_file_path, tarfile=archive_tar, fd=archive_fd, fd_sync=archive_fd_sync)
                    yield self
            if not self._closed:
                archive_fd_sync.close()

    def close(self):
        self.tarfile.close()
        self.fd_sync.close()
        self._closed = True

    async def addfile(self, input_fileobj: ReadableStream, file_key_producer: Awaitable[FileKey], exists: Callable[[FileKey], Awaitable[bool]], overwrite_existing: bool) -> TarFileEntry:
        """Add the TarInfo object 'tarinfo' to the archive. If 'tarinfo' represents
            a non zero-size regular file, the 'fileobj' argument should be a binary file,
            and tarinfo.size bytes are read from it and added to the archive.
            You can create TarInfo objects directly, or by using gettarinfo().
        """
        # Since we don't know the file size ahead of time, write the contents first, then write the header.
        # To account for different tar formats, we create a dummy header with the correct size, write the contents, then write the actual header.
        # As a precaution, we verify that the actual header has the same size as the dummy header.
        tarfile = self.tarfile
        tarfile_fd = self.fd
        dummy_tar_info = TarInfo()
        header_size = len(dummy_tar_info.tobuf(tarfile.format, tarfile.encoding, tarfile.errors))
        header_offset = tarfile.offset
        file_offset = header_offset + header_size
        tarfile.offset = file_offset
        await tarfile_fd.seek(tarfile.offset)
        file_size = await copy(src=input_fileobj, dst=tarfile_fd)
        blocks, remainder = divmod(file_size, BLOCKSIZE)
        if remainder > 0:
            await tarfile_fd.write(NUL * (BLOCKSIZE - remainder))
            blocks += 1
        final_offset = file_offset + blocks * BLOCKSIZE
        # Now write the header
        file_key = await file_key_producer
        if not overwrite_existing and await exists(file_key):
            tarfile.offset = header_offset
            await tarfile_fd.seek(tarfile.offset)
            return TarFileEntry(
                success=False,
                file_key=file_key,
                offset=file_offset,
                size=file_size
            )
        tarfile.offset = header_offset
        await tarfile_fd.seek(tarfile.offset)
        tar_info = TarInfo(name=str(file_key))
        tar_info.size = file_size
        buf = tar_info.tobuf(tarfile.format, tarfile.encoding, tarfile.errors)
        assert len(buf) == header_size, "Header size mismatch"
        await tarfile_fd.write(buf)
        tarfile.offset = final_offset
        await tarfile_fd.seek(tarfile.offset)

        tarfile.members.append(tar_info)
        return TarFileEntry(
            success=True,
            file_key=file_key,
            offset=file_offset,
            size=file_size
        )
