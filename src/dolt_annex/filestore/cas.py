from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Awaitable, List, Tuple
from typing_extensions import Optional
import logging

from dolt_annex.datatypes.async_types import awaited, maybe_await, ReadableStream, AsyncContextManager
from dolt_annex.datatypes.file_io import Path, async_bytes_io
from dolt_annex.datatypes.filestore_config import FilestoreConfig
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.base import FileKey, FileKeyGeneratingReader, FileKeyGenerator
from dolt_annex.filestore.base import FileStore

logger = logging.getLogger(__name__)
class ContentAddressableStorageError(Exception):
    pass

class ContentAddressableStorageKeyMismatchError(ContentAddressableStorageError):
    """Raised when a provided FileKey does not match the computed FileKey of the data being uploaded."""
    pass

@dataclass
class ContentAddressableStorage:
    filestore_config: FilestoreConfig
    file_store: FileStore
    file_key_format: FileKeyType
    alternate_key_formats: list[FileKeyType]

    # In some cases, filestores may store values that are not a content-hash of their keys (such as the "secondary" filestores used by ArchiveFS)
    # In these cases, we don't want to skip validating file integrity and auto-computing keys.
    content_addressed: bool = True

    async def put_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the repo. If the repo is local, this is allowed to move the file (but currently doesn't).
        
        If file_key is not provided, it will be computed.
        """
        return await self.copy_file(file_path, file_key=file_key)
    
    async def copy_file(self, file_path: Path, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an on-disk file to the repo. This must copy the file.
        
        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            file_key = await self.file_key_format.from_file(file_path)
        await self.put_file_object(file_path.open(), awaited(file_key))
        return file_key

    async def put_file_bytes(self, file_bytes: bytes, file_key: Optional[FileKey] = None) -> FileKey:
        """
        Upload an in-memory file to the remote.

        If file_key is not provided, it will be computed.
        """
        if file_key is None:
            if not self.content_addressed:
                raise ContentAddressableStorageError("Cannot compute file key for non-content-addressed filestore")
            file_key = self.file_key_format.from_bytes(file_bytes)
        await self.put_file_object(async_bytes_io(file_bytes), file_key=awaited(file_key))
        return file_key
    
    def file_key_generators(self, extension: Optional[str] = None) -> list[FileKeyGenerator]:
        """Get a list of FileKeyGenerators for all supported key formats."""
        return [
            format.generator(extension=extension) for format in [self.file_key_format, *self.alternate_key_formats]
        ]
    
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey | Awaitable[FileKey]) -> FileKey:
        """
        Insert a file-like object into the repo.
        
        If the key already exists, then based on the configuration,
        this will either do nothing, or validate that the existing content's
        hash matches the key, and replace it if the existing content is corrupted.
        """
        file_key_known = isinstance(file_key, FileKey)
        
        overwrite_existing = False
        if not self.content_addressed:
            if file_key_known:
                file_key = awaited(file_key)
            return await self.file_store.put_file_object(data_source, file_key_producer=file_key)

        if file_key_known and self.filestore_config.verify_existing_files_on_write:
            exists = await maybe_await(self.file_store.exists(file_key))
            if exists:
                is_valid, _ = await self.contains_valid_file(file_key)
                if not is_valid:
                    # If the existing file is corrupted, we replace it with the new content.
                    logger.warning("Existing file with key %s is corrupted, replacing it", file_key)
                    overwrite_existing = True
                else:
                    # If the existing file is valid, we skip writing the new content.
                    logger.info("File with key %s already exists and is valid, skipping write", file_key)
                    return file_key

        if file_key_known:
            file_key = awaited(file_key)
        # We only create key generators if the data source stream is opened.
        # This means that if the write is a no-op because it already exists in
        # the destination, we don't compute alias keys.
        generators: list[FileKeyGenerator] = []
        @asynccontextmanager
        async def open_data_source() -> AsyncGenerator[ReadableStream]:
            nonlocal generators
            generators = self.file_key_generators()
            async with data_source as in_fd:
                yield FileKeyGeneratingReader(in_fd, generators)

        computed_keys: list[FileKey] = []
        original_key: FileKey | None = None
        
        async def file_key_producer() -> FileKey:
            # If any of the computed keys are already in the filestore, we can abort the write
            # by returning the existing key.


            nonlocal computed_keys
            nonlocal original_key
            original_key = await file_key
            computed_keys = [generator.finalize(extension=original_key["extension"]) for generator in generators]

            # We only benefit from this check when there are multiple key types, so exiting early
            # can save us RTTs for remote filestores.
            if len(computed_keys) == 1:
                # TODO: If the computed key doesn't match the supplied key, what should we do?
                return computed_keys[0]
            for computed_key in computed_keys:
                if await maybe_await(self.file_store.exists(computed_key)):
                    return computed_key
            return original_key
 
        file_key = await self.file_store.put_file_object(open_data_source(), file_key_producer=file_key_producer(), overwrite_existing=overwrite_existing)
        if not any(computed_key.same_bytes(original_key) for computed_key in computed_keys):
            raise ContentAddressableStorageKeyMismatchError(f"FileKey mismatch: provided key {file_key} does not match computed keys {computed_keys}")
        
        for alias in computed_keys:
            if alias == file_key:
                continue
            await self.create_alias(old_key=file_key, new_key=alias)

        return file_key
        
    
    async def create_aliases(self, old_key: FileKey, new_key_types: Optional[list[FileKeyType]] = None) -> List[FileKey]:
        """
        Insert a new key that references the same content as an existing key.
        """
        if new_key_types is None:
            new_key_types = self.alternate_key_formats

        alias_keys: List[FileKey] = []
        # If a target key type can be generated from the source key type, we don't need to hash the file contents.
        # If all the target key types can be generated this way, we don't need to read the file contents at all.
        generators: List[FileKeyGenerator] = []
        for format in new_key_types:
            if format.convertable_from(type(old_key)):
                new_key = format.convert_from(old_key)
                alias_keys.append(new_key)
                await self.file_store.create_alias(old_key=old_key, new_key=new_key)
            else:
                generators.append(format.generator(extension=old_key["extension"]))

        if len(generators) == 0:
            return alias_keys
        
        async with self.file_store.get_file_object(old_key) as in_fd:
            reader = FileKeyGeneratingReader(in_fd, generators)
            buffer_size = 1024 * 1024
            while True:
                chunk = await reader.read(buffer_size)
                if not chunk:
                    break

        computed_keys = [generator.finalize() for generator in generators]
        for computed_key in computed_keys:
            await self.file_store.create_alias(old_key=old_key, new_key=computed_key)

        alias_keys.extend(computed_keys)
        return alias_keys

        

    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
        """
        Insert a new key that references the same content as an existing key.
        """
        if self.content_addressed and self.filestore_config.verify_existing_files_on_write:
            exists = await maybe_await(self.file_store.exists(new_key))
            if exists:
                is_valid, _ = await self.contains_valid_file(new_key)
                if not is_valid:
                    # If the existing file is corrupted, we replace it with the new content.
                    logger.warning("Existing file with key %s is corrupted, replacing it", new_key)
                else:
                    # If the existing file is valid, we skip writing the new content.
                    logger.info("File with key %s already exists and is valid, skipping write", new_key)
                    return
        await self.file_store.create_alias(old_key=old_key, new_key=new_key)
    
    async def exists(self, file_key: FileKey) -> bool:
        if self.content_addressed and self.filestore_config.verify_existing_files_on_write:
            is_valid, _ = await self.contains_valid_file(file_key)
            return is_valid
        return await maybe_await(self.file_store.exists(file_key))

    async def contains_valid_file(self, file_key: FileKey) -> Tuple[bool, Optional[FileKey]]:
        """
        Check whether the filestore contains a valid file for the given key. This checks that the file exists, and that its contents match the key.

        If a key mismatch is detected, this returns False, and the computed key of the existing file.
        """
        if not await maybe_await(self.file_store.exists(file_key)):
            return False, None
        # TODO: Files that end in a . currently don't verify correctly, but they're rare in practice.
        if str(file_key)[-1] == '.':
            return True, None
        async with self.file_store.with_file_object(file_key) as in_fd:
            actual_key = await type(file_key).from_fo(in_fd)
            return actual_key.same_bytes(file_key), actual_key

    async def verify_file(self, file_key: FileKey) -> None:
        """
        Assert that a file has the correct bytes by recomputing its key.
        """
        is_valid, actual_key = await self.contains_valid_file(file_key)
        if not is_valid:
            if actual_key is None:
                raise ContentAddressableStorageError(f"File with key {file_key} does not exist in filestore")
            else:
                assert actual_key.same_bytes(file_key), f"File key mismatch: {str(file_key)} was recomputed as {str(actual_key)}"

    async def verify_all_files(self) -> None:
        """
        Verify that all files in the filestore have the correct bytes by recomputing their keys.
        """
        async for file_key, in_fd in self.file_store.get_files():
            async with in_fd as in_fd_opened:
                actual_key = await type(file_key).from_fo(in_fd_opened)
                assert actual_key.same_bytes(file_key)

async def filestore_copy(*, src: ContentAddressableStorage, dst: ContentAddressableStorage, key: FileKey) -> None:
    data_source = src.file_store.with_file_object(key)
    await dst.put_file_object(data_source, file_key=key)
