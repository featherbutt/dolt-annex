#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
import os
import pathlib
from types import TracebackType
from typing_extensions import Awaitable, Optional, Buffer

import aiofiles
from fs.base import FS as FileSystem

from dolt_annex.datatypes import FileKey
from dolt_annex.datatypes.async_types import ReadableFileObject, WritableFileObject
from dolt_annex.datatypes.file_io import FileInfo, SizedBuffer
from dolt_annex.file_keys.base import FileKeyGenerator
from dolt_annex.filestore.cas import ContentAddressableStorage

CHUNK_SIZE = 8092

class FileHandle:
    pass

@dataclass
class ExistingFileHandle(FileHandle, ReadableFileObject):
    readfile: ReadableFileObject
    file_info: FileInfo
    
    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> Awaitable[int]:
        return self.readfile.seek(offset, whence)
    
    def tell(self) -> Awaitable[int]:
        return self.readfile.tell()
    
    def read(self, size: int = -1, /) -> Awaitable[bytes]:
        # This is necessary because tarfile.FileInFile uses None to mean "read all bytes",
        # and -1 will result in zero bytes being read.
        if size == -1:
            return self.readfile.read()
        return self.readfile.read(size)

    def readinto(self, buffer: Buffer, /) -> Awaitable[int]:
        return self.readfile.readinto(buffer)

    def close(self) -> Awaitable[None]:
        return self.readfile.close()

class NewFileHandle(FileHandle, WritableFileObject):
    """A file handle for uploading a new key.
    
    On creation, the file is created in a temporary location.
    After the file is closed, the correct path is computed and the file is moved to the
    final location. This both prevents partial writes and also allows for the file contents
    to be verified before moving it into the annex."""

    writefile: WritableFileObject

    cas: ContentAddressableStorage
    file_key_generator: FileKeyGenerator

    @classmethod
    async def create(cls, temp_fs: FileSystem, cas: ContentAddressableStorage) -> NewFileHandle:
        writefile = await aiofiles.tempfile.NamedTemporaryFile(dir=temp_fs.getsyspath('/'), delete=False, suffix=suffix, buffering=CHUNK_SIZE) # type: ignore
        handle = cls(temp_fs=temp_fs, name=writefile.name, cas=cas, writefile=writefile)
        return handle
    
    def __init__(self, temp_fs: FileSystem, name: str, cas: ContentAddressableStorage, writefile: WritableFileObject):
        self.temp_fs = temp_fs
        self.name = name
        self.cas = cas
        self.writefile = writefile
        self.file_key_generator = cas.file_key_format.generator()

    async def write(self, data: SizedBuffer, /) -> int:
        self.file_key_generator.update(data)
        return await self.writefile.write(data)
    
    async def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return await self.writefile.seek(offset, whence)
    
    async def tell(self) -> int:
        return await self.writefile.tell()
    
    async def read(self, size: int = -1) -> bytes:
        raise NotImplementedError("Read not supported on NewFileHandle")

    @property
    async def file_info(self) -> FileInfo:
        return FileInfo(size=await self.writefile.tell())

    async def close(self) -> None:
        await self.writefile.close()

    def __enter__(self) -> 'NewFileHandle':
        return self
    
    async def __aenter__(self) -> 'NewFileHandle':
        return self
    
    def __exit__(self, type: Optional[type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        self.writefile.close()

    async def __aexit__(self, type: Optional[type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        self.writefile.close()
