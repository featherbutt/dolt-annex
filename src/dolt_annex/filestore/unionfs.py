#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from typing_extensions import override, Any

from dolt_annex.datatypes.file_io import ReadableFileObject
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, MaybeAwaitable, YesNoMaybe, maybe_await

class UnionFS(FileStore):
    """
    A filestore that combines multiple underlying filestores together.

    New files are written to the first filestore, while reads check each filestore in order.
    """

    children: list[FileStore]

    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        """Upload a file-like object to the remote."""
        return await maybe_await(self.children[0].put_file_object(in_fd, file_key))

    @override
    async def get_file_object(self, file_key: FileKey) -> ReadableFileObject:
        """Get a file-like object for a file in the remote by its key."""
        for child in self.children:
            try:
                return await maybe_await(child.get_file_object(file_key))
            except FileNotFoundError:
                continue
        raise FileNotFoundError(f"File with key {file_key} not found in annex.")

    @override
    async def exists(self, file_key: FileKey) -> bool:
        """
        Returns whether the key exists in the filestore.
        """
        for child in self.children:
            match await child.possibly_exists(file_key):
                case YesNoMaybe.YES:
                    return True
                case YesNoMaybe.MAYBE:
                    if await maybe_await(child.exists(file_key)):
                        return True
                case YesNoMaybe.NO:
                    continue
        return False

    @override
    async def possibly_exists(self, file_key: FileKey) -> YesNoMaybe:
        """
        Returns Yes if the file definitely exists in the filestore,
        No if the file definitely does not exist in the filestore,
        and Maybe if the file might exist in the filestore.

        This is often more efficient than calling exists.

        For instance, filestores that use a bloom filter can quickly
        return No or Maybe, at the cost of never returning Yes.
        """
        for child in self.children:
            match await child.possibly_exists(file_key):
                case YesNoMaybe.YES:
                    return YesNoMaybe.YES
                case YesNoMaybe.MAYBE:
                    return YesNoMaybe.MAYBE
                case YesNoMaybe.NO:
                    continue
        return YesNoMaybe.NO
    
    @override
    async def flush(self) -> None:
        """Flush any pending operations to the filestore."""
        for child in self.children:
            await maybe_await(child.flush())


    @override
    @asynccontextmanager
    async def open(self, config: Any) -> AsyncGenerator[None]:
        """Open the filestore, loading or initializing metrics tracking."""
        async with AsyncExitStack() as stack:
            for child in self.children:
                await stack.enter_async_context(child.open(config))
            yield

            await self.flush()

    @override
    def stat(self, file_key: FileKey) -> MaybeAwaitable[FileInfo]:
        for child in self.children:
            if child.exists(file_key):
                return child.stat(file_key)
        raise FileNotFoundError(f"File with key {file_key} not found in annex.")

    @override
    async def fstat(self, file_obj: ReadableFileObject) -> FileInfo:
        for child in self.children:
            try:
                return await maybe_await(child.fstat(file_obj))
            except FileNotFoundError:
                continue
        raise FileNotFoundError("File object not found in any child filestore.")

    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"{self.__class__.__name__}({', '.join(child.type_name() for child in self.children)})"
    