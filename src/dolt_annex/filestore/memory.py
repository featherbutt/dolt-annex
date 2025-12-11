#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MemoryFS is an in-memory filestore useful for testing. It does not persist files
across restarts.
"""

from io import BytesIO
from typing_extensions import override, BinaryIO, cast

from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.file_io import ReadableFileObject
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_io import Path

from .base import FileInfo, FileObject, FileStore

class MemoryFS(FileStore):

    files: dict[bytes, bytes] = {}

    @override
    def put_file(self, file_path: Path, file_key: FileKey) -> None:
        """Move an on-disk file to the annex."""
        with file_path.open() as f:
            self.files[bytes(file_key)] = f.read()
             
    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        """Copy a file-like object into the annex."""
        self.files[bytes(file_key)] = await maybe_await(in_fd.read())

    def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> None:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        self.files[bytes(file_key)] = file_bytes

    async def get_file_object(self, file_key: FileKey) -> BinaryIO:
        if bytes(file_key) not in self.files:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return BytesIO(self.files[bytes(file_key)])
        
    @override
    def stat(self, file_key: FileKey) -> FileInfo:
         return FileInfo(size=len(self.files[bytes(file_key)]))

    @override
    def fstat(self, file_obj: FileObject) -> FileInfo:
         b = cast(BytesIO, file_obj)
         return FileInfo(size=len(b.getvalue()))

    @override
    def exists(self, file_key: FileKey) -> bool:
        return file_key in self.files
