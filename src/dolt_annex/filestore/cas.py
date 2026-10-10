from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import List, Tuple
from typing_extensions import Optional
import logging

from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.errors import ContentAddressableStorageError
from dolt_annex.datatypes.filestore_config import FilestoreConfig
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.base import FileKey, FileKeyGeneratingReader, FileKeyGenerator
from dolt_annex.filestore.base import FileStore
from dolt_annex.datatypes.file_stream import KeyedFileStream, KnownKeyFileStream, known_key_source

logger = logging.getLogger(__name__)

@dataclass
class ContentAddressableStorage:
    filestore_config: FilestoreConfig
    file_store: FileStore
    supported_key_formats: list[FileKeyType]

    # In some cases, filestores may store values that are not a content-hash of their keys (such as the "secondary" filestores used by ArchiveFS)
    # In these cases, we don't want to skip validating file integrity and auto-computing keys.
    content_addressed: bool = True

    def file_key_generators(self, extension: Optional[str] = None) -> list[FileKeyGenerator]:
        """Get a list of FileKeyGenerators for all supported key formats."""
        return [
            format.generator(extension=extension) for format in self.supported_key_formats
        ]
    
    async def put_file[T: FileKey](self, data_source: KeyedFileStream[T]) -> FileStore.PutFileResult:
        """
        Insert a file-like object into the repo.
        
        If the key already exists, then based on the configuration,
        this will either do nothing, or validate that the existing content's
        hash matches the key, and replace it if the existing content is corrupted.
        """
        if not self.content_addressed:
            return await self.file_store.put_file_object(data_source)

        generators = self.file_key_generators()
        overwrite_existing = False
        if isinstance(data_source, KnownKeyFileStream):
            if self.filestore_config.verify_existing_files_on_write:
                exists = await self.file_store.exists(data_source.known_key)
                if exists:
                    is_valid, _ = await self.contains_valid_file(data_source.known_key)
                    if not is_valid:
                        # If the existing file is corrupted, we replace it with the new content.
                        logger.warning("Existing file with key %s is corrupted, replacing it", data_source.known_key)
                        overwrite_existing = True
                    else:
                        # If the existing file is valid, we skip writing the new content.
                        logger.info("File with key %s already exists and is valid, skipping write", data_source.known_key)
                        return FileStore.PutFileResult(file_key=data_source.known_key, uuids=[self.file_store.uuid] if self.file_store.uuid else [])

        file_key_from_source: Optional[FileKey] = None

        async def _file_key_producer() -> FileKey:
            file_key = await data_source.file_key()
            nonlocal file_key_from_source
            file_key_from_source = file_key
        
            # If the file already exists under one of its aliases, return that one.
            # This will cause the filestore to avoid storing a duplicate file.
            # We only benefit from this check when there are multiple key types, so exiting early
            # can save us RTTs for remote filestores.
            computed_keys = [generator.finalize(extension=file_key["extension"]) for generator in generators]

            for computed_key in computed_keys:
                if await self.file_store.exists(computed_key):
                    return computed_key
            return file_key

        new_data_source = KeyedFileStream(
            data_source = FileKeyGeneratingReader.new(data_source, generators),
            file_key_producer=_file_key_producer
        )

        result_file_key, _ = await self.file_store.put_file_object(new_data_source, overwrite_existing=overwrite_existing)

        computed_keys = [generator.finalize(extension=result_file_key["extension"]) for generator in generators]
        for alias in computed_keys:
            if alias == result_file_key:
                continue
            await self.create_alias(old_key=result_file_key, new_key=alias)

        # In certain cases, the file key from the data source may differ from the file key that was actually stored.
        # In that case, we create an alias from the stored file key to the original file key from the data source.
        # This ensures that the provided file key will always exist in the filestore afterward.
        if file_key_from_source:
            if file_key_from_source != result_file_key and file_key_from_source not in computed_keys:
                await self.create_alias(old_key=result_file_key, new_key=file_key_from_source)
            result_file_key = file_key_from_source
        
        return FileStore.PutFileResult(file_key=result_file_key, uuids=[self.file_store.uuid] if self.file_store.uuid else [])

    def with_file_stream(self, key: FileKey) -> KeyedFileStream:
        return known_key_source(self.file_store.with_file_object(key), key)
    
    async def create_aliases(self, old_key: FileKey, new_key_types: Optional[list[FileKeyType]] = None) -> List[FileKey]:
        """
        Insert a new key that references the same content as an existing key.
        """
        if new_key_types is None:
            new_key_types = self.supported_key_formats

        alias_keys: List[FileKey] = []
        # If a target key type can be generated from the source key type, we don't need to hash the file contents.
        # If all the target key types can be generated this way, we don't need to read the file contents at all.
        generators: List[FileKeyGenerator] = []
        for format in new_key_types:
            if format is type(old_key):
                # The new key would just be identical to the old key.
                alias_keys.append(old_key)
                continue
            if format.convertable_from(type(old_key)):
                new_key = format.convert_from(old_key)
                alias_keys.append(new_key)
                await self.file_store.create_alias(old_key=old_key, new_key=new_key)
            else:
                generators.append(format.generator(extension=old_key["extension"]))

        if len(generators) == 0:
            return alias_keys
        
        async with self.file_store.with_file_object(old_key) as in_fd:
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

    async def get_files(self, prefix: bytes = b"") -> AsyncGenerator[KnownKeyFileStream]:
        async for known_key_stream in self.file_store.get_files(prefix):
            yield known_key_source(known_key_stream, known_key_stream.known_key)

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
        async for data_source in self.file_store.get_files():
            async with data_source as in_fd_opened:
                actual_key = await type(data_source.known_key).from_fo(in_fd_opened)
                assert actual_key.same_bytes(data_source.known_key)

async def filestore_copy(*, src: ContentAddressableStorage, dst: ContentAddressableStorage, key: FileKey) -> None:
    await dst.put_file(src.with_file_stream(key))