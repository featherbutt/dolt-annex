#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
import abc
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import wraps
import inspect
from typing import TYPE_CHECKING, Awaitable, Tuple
import logging

from dolt_annex.datatypes.async_types import MaybeAwaitable, awaited, maybe_await, AwaitOrEnter, ReadableFileObject, ReadableStream, WritableStream, AsyncContextManager
from dolt_annex.datatypes.common import YesNoMaybe
from dolt_annex.datatypes.file_io import FileInfo, Path, async_bytes_io
from dolt_annex.datatypes.pydantic import AbstractBaseModel
from dolt_annex.file_keys import FileKey

if TYPE_CHECKING:
    from dolt_annex.datatypes.config import Config

logger = logging.getLogger(__name__)

class FileStoreError(Exception):
    pass

def wrap_errors(*, wrap: type[Exception], into: type[Exception]):
    """Decorator to wrap exceptions of one type as another type."""
    def decorator(func):
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except wrap as e:
                    raise into from e
            return async_wrapper
        elif inspect.isgeneratorfunction(func):
            @wraps(func)
            def generator_wrapper(*args, **kwargs):
                try:
                    yield from func(*args, **kwargs)
                except wrap as e:
                    raise into from e
            return generator_wrapper
        elif inspect.isasyncgenfunction(func):
            @wraps(func)
            async def asyncgen_wrapper(*args, **kwargs):
                try:
                    async for item in func(*args, **kwargs):
                        yield item
                except wrap as e:
                    raise into from e
            return asyncgen_wrapper
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except wrap as e:
                    raise into from e
            return wrapper
    return decorator

class FileStore(abc.ABC):

    async def put_file(self, file_path: Path, file_key: FileKey) -> None:
        """
        Insert an on-disk file to the repo. If the repo is local, this is allowed to move the file.
        """
        await self.copy_file(file_path, file_key)

    async def copy_file(self, file_path: Path, file_key: FileKey) -> None:
        """
        Copy an on-disk file to the remote. If the repo is local, this must copy the file.
        """
        await maybe_await(self.put_file_object(file_path.open(), awaited(file_key)))

    async def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> None:
        """
        Insert an in-memory file to the remote.
        """
        await maybe_await(self.put_file_object(async_bytes_io(file_bytes), file_key_producer=awaited(file_key)))
    
    @abstractmethod
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key_producer: Awaitable[FileKey], overwrite_existing: bool = False) -> FileKey:
        """Insert a file-like object into the remote. If the key already exists, the filestore *must* replace the existing content with the new content."""

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
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        """
        Insert a new key that references the same content as an existing key.

        The default implementation reads the content for old_key and writes it to new_key.
        For most filestores, this is inefficient; subclasses should override this method to
        avoid transferring data over the network and duplicating storage.
        """
        logger.info(f"alias {old_key} -> {new_key}")
        await self.put_file_object(self.get_file_object(old_key), awaited(new_key))
    
    class GetFilesNotImplementedError(NotImplementedError):
        pass

    def get_files(self, prefix: bytes = b"") -> AsyncGenerator[Tuple[FileKey, AwaitOrEnter[ReadableStream]]]:
        """
        Iterate over all file keys in the filestore. This is primarily intended for testing and debugging
        and it not required to be implemented by all filestores.
        """
        raise FileStore.GetFilesNotImplementedError(f"{self.__class__.__name__} does not implement get_files.")
    
    def delete(self, key: FileKey) -> None:
        """
        Remove a file from a filestore if supported. Is not guarenteed to free space, and may not play well with aliases.
        """
        pass

async def copy(*, src: ReadableStream, dst: WritableStream, buffer_size=16384) -> int:
    bytes_copied = 0
    while True:
        buf = await src.read(buffer_size)
        if not buf:
            return bytes_copied
        await dst.write(buf)
        bytes_copied += len(buf)

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
