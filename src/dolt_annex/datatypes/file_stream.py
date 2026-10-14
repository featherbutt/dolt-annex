import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Awaitable, Optional

from dolt_annex.datatypes.async_types import AwaitOrEnter, ReadableStream, awaitable, awaitable_func
from dolt_annex.datatypes.errors import ContentAddressableStorageError
from dolt_annex.datatypes.file_io import Path, Pipe, async_bytes_io
from dolt_annex.file_keys.base import FileKey, FileKeyGeneratingReader

@dataclass(kw_only=True)
class KeyedFileStream[T: FileKey](AwaitOrEnter[ReadableStream]):
    data_source: AwaitOrEnter[ReadableStream]
    file_key_producer: Callable[[], Awaitable[T]]
    _produced_file_key: Optional[T] = None

    def __await__(self):
        return self.data_source.__await__()

    async def __aenter__(self) -> ReadableStream:
        return await self.data_source.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.data_source.__aexit__(exc_type, exc_val, exc_tb)

    async def file_key(self) -> T:
        if self._produced_file_key is None:
            self._produced_file_key = await self.file_key_producer()
        return self._produced_file_key

@dataclass(kw_only=True)
class KnownKeyFileStream[T: FileKey](KeyedFileStream[T]):
    known_key: T

def file_source[T: FileKey](data_source: AwaitOrEnter[ReadableStream], file_key_type: type[T], extension: Optional[str] = None) -> KeyedFileStream[T]:
    file_key_generator = file_key_type.generator(extension=extension)
    generators = [file_key_generator]
    data_source = FileKeyGeneratingReader.new(data_source, generators)
    return KeyedFileStream[T](
        data_source=data_source,
        file_key_producer=awaitable_func(file_key_generator.finalize)
    )

def path_file_source[T: FileKey](path: Path, file_key_type: type[T], extension: Optional[str] = None) -> KeyedFileStream[T]:
    return file_source(path.open(), file_key_type, extension=extension)

def bytes_file_source[T: FileKey](data: bytes, file_key_type: type[T], extension: Optional[str] = None) -> KeyedFileStream[T]:
    return file_source(async_bytes_io(data), file_key_type, extension=extension)

def known_key_source_nocheck[T: FileKey](data_source: AwaitOrEnter[ReadableStream], file_key: T) -> KnownKeyFileStream[T]:
    return KnownKeyFileStream[T](
        data_source=data_source,
        file_key_producer=awaitable(file_key),
        known_key=file_key
    )

def known_key_source[T: FileKey](data_source: AwaitOrEnter[ReadableStream], file_key: T) -> KnownKeyFileStream[T]:
    file_key_generator = type(file_key).generator(extension=file_key["extension"])
    generators = [file_key_generator]
    data_source = FileKeyGeneratingReader.new(data_source, generators)
    async def finalize_file_key():
        computed_file_key = file_key_generator.finalize()
        if not file_key.same_bytes(computed_file_key):
            raise ContentAddressableStorageError(f"FileKey mismatch: provided key {file_key} does not match computed key {computed_file_key}")
        return file_key
    return KnownKeyFileStream[T](
        data_source=data_source,
        file_key_producer=finalize_file_key,
        known_key=file_key
    )

def fork_readable_stream(source: KeyedFileStream, num_forks: int = 2, max_buffer_size: int = 1 << 20, event_loop: Optional[asyncio.AbstractEventLoop] = None) -> list[KeyedFileStream]:
    if event_loop is None:
        event_loop = asyncio.get_event_loop()
        
    pipes = [Pipe(event_loop, max_buffer_size) for _ in range(num_forks)]
    pipe_context_managers = [pipe.open() for pipe in pipes]

    async def feed_pipes():
        async with source as src:
            while True:
                chunk = await src.read(max_buffer_size)
                if not chunk:
                    break
                for pipe in pipes:
                    await pipe.write(chunk)
        for pipe in pipes:
            await pipe.write(b"")
            pipe.done.set()

    event_loop.create_task(feed_pipes())

    return [KeyedFileStream(data_source=pipe, file_key_producer=source.file_key) for pipe in pipe_context_managers]
