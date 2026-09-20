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
from dolt_annex.datatypes.async_types import ReadableFileObject, WritableFileObject, SizedBuffer
from dolt_annex.datatypes.file_io import FileInfo
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
