#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Optional
from typing_extensions import override, Any

from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream
from dolt_annex.datatypes.loader import Named
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_stream import KeyedFileStream, KnownKeyFileStream, fork_readable_stream

from .base import FileInfo, FileStore, FileStoreError, FileStoreModel, YesNoMaybe, maybe_await

class UnionFS(FileStore):
    """
    A filestore that combines multiple underlying filestores together.

    New files are written to the first filestore, while reads check each filestore in order.
    """

    children: list[FileStore]

    write_to_all_children: bool = False

    def __init__(self, *, uuid: Optional[FileStore.Id], children: list[FileStore], write_to_all_children: bool = False):
        self.uuid = uuid
        self.children = children
        self.write_to_all_children = write_to_all_children

    @override
    async def put_file_object(self, data_source: KeyedFileStream, overwrite_existing: bool = False) -> FileStore.PutFileResult:
        """Upload a file-like object to the remote."""
        if self.write_to_all_children:
            uuids = []
            child_streams = fork_readable_stream(data_source, num_forks=len(self.children))
            for child, child_stream in zip(self.children, child_streams):
                _, child_uuids = await child.put_file_object(child_stream, overwrite_existing=overwrite_existing) # TODO: Don't need to put synchroniously; use a worker pool
                uuids.extend(child_uuids)
        else:
            _, uuids = await self.children[0].put_file_object(data_source, overwrite_existing=overwrite_existing)
        return FileStore.PutFileResult(file_key=await data_source.file_key(), uuids=list(uuids) + ([self.uuid] if self.uuid else []))

    @override
    @await_or_enter
    async def with_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""
        for child in self.children:
            try:
                async with child.with_file_object(file_key) as file_obj:
                    yield file_obj
                    return
            except (FileNotFoundError, FileStoreError):
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
    async def stat(self, file_key: FileKey) -> FileInfo:
        for child in self.children:
            if await child.exists(file_key):
                return await child.stat(file_key)
        raise FileNotFoundError(f"File with key {file_key} not found in annex.")

    @override
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        for child in self.children:
            try:
                return await maybe_await(child.fstat(file_obj))
            except FileNotFoundError:
                continue
        raise FileNotFoundError("File object not found in any child filestore.")
    
    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        found = False
        for child in self.children:
            if await child.exists(old_key):
                await child.create_alias(old_key, new_key)
                found = True
                if not self.write_to_all_children:
                    return
        if not found:
            raise FileNotFoundError(f"File with key {old_key} not found in annex.")

    @override
    async def get_files(self, prefix: bytes = b"") -> AsyncGenerator[KnownKeyFileStream]:
        for child in self.children:
            async for known_key_stream in child.get_files(prefix):
                yield known_key_stream

    @override
    def delete(self, key: FileKey):
        for child in self.children:
            child.delete(key)

class UnionFSModel(FileStoreModel):
    children: list[Named[FileStoreModel]]
    write_to_all_children: bool = False

    @override
    @asynccontextmanager
    async def open(self, config: Any) -> AsyncGenerator[UnionFS]:
        children_filestores = []
        async with AsyncExitStack() as stack:
            for child_model in self.children:
                child_instance = await stack.enter_async_context(child_model.open(config))
                children_filestores.append(child_instance)
            yield UnionFS(
                uuid=self.uuid,
                children=children_filestores,
                write_to_all_children=self.write_to_all_children
            )

    @override
    def type_name(self) -> str:
        """Get the type name of the filestore. Used in tests."""
        return f"{self.__class__.__name__}({', '.join(child.type_name() for child in self.children)})"
    