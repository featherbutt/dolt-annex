#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path
from typing import Self
from typing_extensions import AsyncContextManager

from dolt_annex.datatypes.async_utils import MaybeAwaitable, maybe_await
from dolt_annex.datatypes.common import YesNoMaybe
from dolt_annex.datatypes.file_io import FileInfo, FileObject, ReadableFileObject, WritableFileObject
from dolt_annex.datatypes.pydantic import AbstractBaseModel
from dolt_annex.file_keys import FileKey

class FileStore(AbstractBaseModel):

    def put_file(self, file_path: Path, file_key: FileKey) -> MaybeAwaitable[None]:
        """
        Upload an on-disk file to the repo. If the repo is local, this is allowed to move the file.
        """
        return self.copy_file(file_path, file_key)
    
    async def copy_file(self, file_path: Path, file_key: FileKey) -> None:
        """
        Upload an on-disk file to the remote. If the repo is local, this must copy the file.
        """
        with open(file_path, 'rb') as fd:
            return await maybe_await(self.put_file_object(fd, file_key))

    def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> MaybeAwaitable[None]:
        """
        Upload an in-memory file to the remote.
        """
        return self.put_file_object(BytesIO(file_bytes), file_key=file_key)
    
    @abstractmethod
    def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> MaybeAwaitable[None]:
        """Upload a file-like object to the remote."""

    @abstractmethod
    def get_file_object(self, file_key: FileKey) -> MaybeAwaitable[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""
    
    def with_file_object(self, file_key: FileKey) -> AsyncContextManager[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""
        @asynccontextmanager
        async def inner() -> AsyncGenerator[ReadableFileObject]:
            file = await maybe_await(self.get_file_object(file_key))
            yield file
            await maybe_await(file.close())
        return inner()

    @abstractmethod
    def exists(self, file_key: FileKey) -> MaybeAwaitable[bool]:
        """
        Returns whether the key exists in the filestore.
        """

    @abstractmethod
    def stat(self, file_key: FileKey) -> MaybeAwaitable[FileInfo]:
        """
        Returns information about a file-like object previously returned by get_file_object.
        """

    @abstractmethod
    def fstat(self, file_obj: FileObject) -> MaybeAwaitable[FileInfo]:
        """
        Returns information about a file-like object previously returned by get_file_object.
        """

    async def possibly_exists(self, file_key: FileKey) -> YesNoMaybe:
        """
        If false, the file definitely does not exist in the filestore.
        This is often more efficient than calling exists.
        """
        if await maybe_await(self.exists(file_key)):
            return YesNoMaybe.YES
        return YesNoMaybe.NO

    @asynccontextmanager
    async def open(self, config: 'Config') -> AsyncGenerator[Self]:
        """
        Open the filestore for use. This may involve setting up connections, opening files, etc.

        Returns a context manager that yields the opened filestore instance.
        """ 

        yield self
        await self.flush()
    

    async def flush(self) -> None:
        """Flush any pending operations to the filestore."""

async def copy(*, src: ReadableFileObject, dst: WritableFileObject, buffer_size=4096):
    while True:
        buf = await maybe_await(src.read(buffer_size))
        if not buf:
            break
        await maybe_await(dst.write(buf))
