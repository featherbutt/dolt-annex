#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
import abc
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from dolt_annex.datatypes.async_types import MaybeAwaitable, maybe_await, AwaitOrEnter, ReadableFileObject, ReadableStream, WritableStream, AsyncContextManager
from dolt_annex.datatypes.async_utils import Result
from dolt_annex.datatypes.common import YesNoMaybe
from dolt_annex.datatypes.file_io import FileInfo, Path, async_bytes_io
from dolt_annex.datatypes.pydantic import AbstractBaseModel
from dolt_annex.file_keys import FileKey

if TYPE_CHECKING:
    from dolt_annex.datatypes.config import Config

class FileStore(abc.ABC):

    def put_file(self, file_path: Path, file_key: FileKey) -> MaybeAwaitable[Result[None]]:
        """
        Upload an on-disk file to the repo. If the repo is local, this is allowed to move the file.
        """
        return self.copy_file(file_path, file_key)

    async def copy_file(self, file_path: Path, file_key: FileKey) -> Result[None]:
        """
        Upload an on-disk file to the remote. If the repo is local, this must copy the file.
        """
        return await maybe_await(self.put_file_object(file_path.open(), file_key))

    def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> MaybeAwaitable[Result[None]]:
        """
        Upload an in-memory file to the remote.
        """
        return self.put_file_object(async_bytes_io(file_bytes), file_key=file_key)

    @abstractmethod
    def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey) -> MaybeAwaitable[Result[None]]:
        """Upload a file-like object to the remote."""

    @abstractmethod
    def get_file_object(self, file_key: FileKey) -> AwaitOrEnter[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""

    def with_file_object(self, file_key: FileKey) -> AsyncContextManager[ReadableStream]:
        """Get a file-like object for a file in the remote by its key."""
        return self.get_file_object(file_key)

    async def get_file_bytes(self, file_key: FileKey) -> bytes:
        """
        Get the contents of a file in the remote by its key.
        """
        async with self.with_file_object(file_key) as fd:
            return await fd.read()

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
    def fstat(self, file_obj: ReadableStream) -> MaybeAwaitable[FileInfo]:
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

    def flush(self) -> MaybeAwaitable[None]:
        """Flush any pending operations to the filestore."""

    @abstractmethod
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> Result[None]:
        """
        Insert a new key that references the same content as an existing key.

        The default implementation reads the content for old_key and writes it to new_key.
        For most filestores, this is inefficient; subclasses should override this method to
        avoid transferring data over the network and duplicating storage.
        """
        return await maybe_await(self.put_file_object(self.get_file_object(old_key), new_key))

async def copy(*, src: ReadableStream, dst: WritableStream, buffer_size=16384):
    while True:
        buf = await src.read(buffer_size)
        if not buf:
            break
        await dst.write(buf)

class FileStoreModel(AbstractBaseModel):
    """
    Subclasses must implement either open() or create().
    """
    
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[FileStore]:
        """
        Open the filestore for use. This may involve setting up connections, opening files, etc.

        Returns a context manager that yields the opened filestore instance.
        """
        filestore = self.create(config)
        try:
            yield filestore
        finally:
            await maybe_await(filestore.flush())

    def create(self, config: Config) -> FileStore:
        """
        Create a new instance of the filestore from the model.
        """
        raise NotImplementedError()

    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return self.__class__.__name__