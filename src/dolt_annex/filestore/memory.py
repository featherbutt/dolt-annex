#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MemoryFS is an in-memory filestore useful for testing. It does not persist files
across restarts.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from io import BytesIO
from typing_extensions import override, BinaryIO, cast

from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.file_io import FileObject, ReadableFileObject
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_io import Path

from .base import FileInfo, FileStore

class MemoryFS(FileStore):

    files: dict[bytes, bytes] = {}

    # There is no technical requirement for a MemoryFS to be "opened", but
    # many tests use MemoryFS, and we want to guarentee that we open filestores
    # before using them.
    # TODO: Enforce this in the type system instead.
    _is_open: bool = False

    @override
    def put_file(self, file_path: Path, file_key: FileKey) -> None:
        """Move an on-disk file to the annex."""
        assert self._is_open, "MemoryFS must be opened before use."
        with file_path.open() as f:
            self.files[bytes(file_key)] = f.read()
             
    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        """Copy a file-like object into the annex."""
        assert self._is_open, "MemoryFS must be opened before use."
        self.files[bytes(file_key)] = await maybe_await(in_fd.read())

    def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> None:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        assert self._is_open, "MemoryFS must be opened before use."
        self.files[bytes(file_key)] = file_bytes

    async def get_file_object(self, file_key: FileKey) -> BinaryIO:
        assert self._is_open, "MemoryFS must be opened before use."
        if bytes(file_key) not in self.files:
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return BytesIO(self.files[bytes(file_key)])
        
    @override
    def stat(self, file_key: FileKey) -> FileInfo:
        assert self._is_open, "MemoryFS must be opened before use."
        return FileInfo(size=len(self.files[bytes(file_key)]))

    @override
    def fstat(self, file_obj: FileObject) -> FileInfo:
        assert self._is_open, "MemoryFS must be opened before use."
        b = cast(BytesIO, file_obj)
        return FileInfo(size=len(b.getvalue()))

    @override
    def exists(self, file_key: FileKey) -> bool:
        assert self._is_open, "MemoryFS must be opened before use."
        return bytes(file_key) in self.files

    @asynccontextmanager
    async def open(self, config: 'Config') -> AsyncGenerator[None]:
        """
        Open the filestore for use. This may involve setting up connections, opening files, etc.

        Returns a context manager that yields the opened filestore instance.
        """
        _is_open = self._is_open
        self._is_open = True
        try:
            yield
        finally:
            self._is_open = _is_open