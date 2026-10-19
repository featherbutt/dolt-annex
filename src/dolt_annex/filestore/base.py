#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
import abc
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import wraps
import inspect
import pathlib
from typing import TYPE_CHECKING, Optional, Sequence
import logging
import typing
from dolt_annex.datatypes.errors import StorageError
from uuid import UUID

from dolt_annex.datatypes.async_types import AwaitOrEnter, ReadableFileObject, ReadableStream, WritableStream
from dolt_annex.datatypes.file_io import FileInfo, async_bytes_io
from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.pydantic import AbstractBaseModel
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_stream import KeyedFileStream, KnownKeyFileStream, known_key_source_nocheck

if TYPE_CHECKING:
    from dolt_annex.config.base import Config

logger = logging.getLogger(__name__)

class FileStoreError(StorageError):
    """Raised when an file store operation fails."""
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
                    gen = func(*args, **kwargs)
                    try:
                        async for item in gen:
                            yield item
                    finally:
                        await gen.aclose()
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

@dataclass
class FileStoreBaseParams:
    """
    Parameters supplied to all FileStoreModels and used by all FileStore implementations.
    """

    uuid: Optional[FileStore.Id] = None
    content_addressed: bool = True

class FileStore(abc.ABC):

    Id = UUID

    uuid: Optional[Id] = None
    content_addressed: bool = True

    def __init__(self, model: FileStoreBaseParams):
        self.uuid = model.uuid
        self.content_addressed = model.content_addressed

    class PutFileResult(typing.NamedTuple):
        file_key: FileKey
        uuids: Sequence[FileStore.Id]

    async def put_file_bytes(self, file_bytes: bytes, file_key: FileKey) -> PutFileResult:
        """
        Insert an in-memory file to the remote.
        """
        return await self.put_file_object(known_key_source_nocheck(async_bytes_io(file_bytes), file_key=file_key))
    
    @abstractmethod
    async def put_file_object(self, data_source: KeyedFileStream, overwrite_existing: bool = False) -> PutFileResult:
        """Insert a file-like object into the remote. If the key already exists, the filestore *must* replace the existing content with the new content."""

    def with_file_stream(self, file_key: FileKey) -> KeyedFileStream:
        """Get a file-like stream for a file in the remote by its key."""
        return known_key_source_nocheck(self.with_file_object(file_key), file_key)
    
    @abstractmethod
    def with_file_object(self, file_key: FileKey) -> AwaitOrEnter[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""

    async def get_file_bytes(self, file_key: FileKey) -> bytes:
        """
        Get the contents of a file in the remote by its key.
        """
        async with self.with_file_object(file_key) as fd:
            return await fd.read()

    @abstractmethod
    async def exists(self, file_key: FileKey) -> bool:
        """
        Returns whether the key exists in the filestore.
        """

    @abstractmethod
    async def stat(self, file_key: FileKey) -> FileInfo:
        """
        Returns information about a file-like object previously returned by with_file_object.
        """

    @abstractmethod
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        """
        Returns information about a file-like object previously returned by with_file_object.
        """

    async def flush(self) -> None:
        """Flush any pending operations to the filestore."""

    @abstractmethod
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        """
        Insert a new key that references the same content as an existing key.

        The default implementation reads the content for old_key and writes it to new_key.
        For most filestores, this is inefficient; subclasses should override this method to
        avoid transferring data over the network and duplicating storage.
        """
        logger.debug(f"alias {old_key} -> {new_key}")
        data_source = known_key_source_nocheck(self.with_file_object(old_key), new_key)
        await self.put_file_object(data_source, overwrite_existing=False)
    
    class GetFilesNotImplementedError(NotImplementedError):
        pass

    def get_files(self, prefix: bytes = b"") -> AsyncGenerator[KnownKeyFileStream]:
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

async def copy(*, src: ReadableStream, dst: WritableStream, buffer_size=1 << 20) -> int:
    bytes_copied = 0
    while True:
        buf = await src.read(buffer_size)
        if not buf:
            return bytes_copied
        await dst.write(buf)
        bytes_copied += len(buf)

class FileStoreModel(Loadable, AbstractBaseModel, FileStoreBaseParams, extension="filestore", config_dir=pathlib.Path("filestores")):
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
            await filestore.flush()

    def create(self, config: Config) -> FileStore:
        """
        Create a new instance of the filestore from the model.
        """
        raise NotImplementedError()

    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return self.__class__.__name__
