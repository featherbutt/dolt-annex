from __future__ import annotations

from dataclasses import dataclass
from io import BufferedReader, BufferedWriter
import os
import pathlib
import tempfile
from typing_extensions import Optional, Buffer

from fs.base import FS as FileSystem

from dolt_annex.datatypes import FileKey
from dolt_annex.datatypes.file_io import FileInfo, ReadableFileObject, WritableFileObject
from dolt_annex.filestore.cas import ContentAddressableStorage


CHUNK_SIZE = 8092

class FileHandle:
    pass

@dataclass
class ExistingFileHandle(FileHandle, ReadableFileObject):
    readfile: BufferedReader
    file_info: FileInfo
    
    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self.readfile.seek(offset, whence)
    
    def read(self, size: int = -1, /) -> bytes:
        return self.readfile.read(size)

    def close(self) -> None:
        self.readfile.close()

    def __enter__(self) -> 'ExistingFileHandle':
        return self
    
    def __exit__(self, type: Optional[type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        self.close()

class NewFileHandle(FileHandle, WritableFileObject):
    """A file handle for uploading a new key.
    
    On creation, the file is created in a temporary location.
    After the file is closed, the correct path is computed and the file is moved to the
    final location. This both prevents partial writes and also allows for the file contents
    to be verified before moving it into the annex."""

    writefile: BufferedWriter

    key: FileKey
    suffix: str

    cas: ContentAddressableStorage

    def __init__(self, temp_fs: FileSystem, cas: ContentAddressableStorage, key: FileKey):
        self.temp_fs = temp_fs
        self.cas = cas
        self.key = key
        self.suffix = pathlib.Path(str(key)).suffix[1:]  # Remove the leading dot
        self.writefile = tempfile.NamedTemporaryFile(dir=self.temp_fs.getsyspath('/'), delete=False, suffix=self.suffix, buffering=CHUNK_SIZE) # type: ignore
        
    def write(self, data: Buffer, /) -> int:
        return self.writefile.write(data)
    
    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self.writefile.seek(offset, whence)
    
    def read(self, size: int = -1) -> bytes:
        raise NotImplementedError("Read not supported on NewFileHandle")

    @property
    def file_info(self) -> FileInfo:
        return FileInfo(size=self.writefile.tell())

    def close(self) -> None:
        self.writefile.close()

    def __enter__(self) -> 'NewFileHandle':
        return self
    
    def __exit__(self, type: Optional[type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        self.close()
