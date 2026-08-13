#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from asyncio import Future
import asyncio
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
import contextvars
from dataclasses import dataclass
from io import BytesIO
import pathlib
from typing import Optional
from aiofiles.threadpool.binary import AsyncFileIO
from aiofiles.base import AiofilesContextManager
import fs.copy
from typing_extensions import BinaryIO, Literal

import fs.move
import fs.errors
from fs.base import FS
from fs.osfs import OSFS

from dolt_annex.datatypes.async_types import AwaitOrEnter, MaybeAwaitable, ReadableStream, maybe_await

@dataclass
class FileInfo:
    size: int | None

def async_open(fd: MaybeAwaitable[BinaryIO]) -> AwaitOrEnter[AsyncFileIO]:
    """
    Wrap a synchronous file object so it can be used asynchronously
    or in an async context manager.
    """
    async def async_file_io():
        return AsyncFileIO(await maybe_await(fd), None, None)
    return AiofilesContextManager(async_file_io())

def async_bytes_io(data: bytes) -> AwaitOrEnter[AsyncBytesIO]:
    # We use a Future instead of a coroutine here because if the ContextManager is never entered,
    # the wrapped awaitable will never be awaited. We want to avoid creating coroutines that
    # might not be awaited.
    f = Future[AsyncBytesIO]()
    f.set_result(AsyncBytesIO(data))
    return AiofilesContextManager(f)

class AsyncBytesIO(AsyncFileIO):
    """
    A wrapper around BytesIO that provides an asynchronous file interface,
    while also providing access to the underlying bytes.
    """

    data: bytes

    def __init__(self, data: bytes) -> None:
        super().__init__(BytesIO(data), None, None)
        self.data = data

file_system_context = contextvars.ContextVar[FS]("file_system_context", default=OSFS('.'))

@dataclass
class Path:
    """
    A path in a specific filesystem.
    """

    fs: FS
    path: pathlib.Path = pathlib.Path('/')

    def __init__(self, file_system: FS, path: pathlib.Path | str | None = None) -> None:
        self.fs = file_system
        if path is None:
            self.path = pathlib.Path('/')
        elif isinstance(path, str):
            self.path = pathlib.Path(path)
        else:
            self.path = path

    def __fspath__(self) -> str:
        return self.path.as_posix()
    
    def as_posix(self) -> str:
        return self.path.as_posix()

    def __truediv__(self, key: str) -> 'Path':
        return Path(self.fs, self.path / key)

    def exists(self) -> bool:
        return self.fs.exists(self.path.as_posix())
    
    def is_dir(self) -> bool:
        return self.fs.isdir(self.path.as_posix())

    def open(self, mode: Literal['rb', 'wb', 'ab', 'r+b'] = 'rb') -> AwaitOrEnter[AsyncFileIO]:
        # Avoid opening the file synchronously; wait for the async context instead.
        # This helps ensure that every file open is matched with a file close.
        async def open_inner() -> BinaryIO:
            return self.open_sync(mode)
        return async_open(open_inner())

    def open_sync(self, mode: Literal['rb', 'wb', 'ab', 'r+b'] = 'rb') -> BinaryIO:
        return self.fs.openbin(self.path.as_posix(), mode)
    
    def touch(self) -> None:
        self.fs.create(self.path.as_posix())

    @property
    def parent(self) -> Path:
        return Path(self.fs, self.path.parent)

    def mkdirs(self, exist_ok: bool = False) -> None:
        self.fs.makedirs(self.path.as_posix(), recreate=exist_ok)

    def rename(self, target: Path, mkdirs: bool = True) -> None:
        if mkdirs:
            target.parent.mkdirs(exist_ok=True)
        fs.move.move_file(self.fs, self.path.as_posix(), target.fs, target.path.as_posix())

    def upload(self, in_fd: BinaryIO) -> None:
        self.fs.upload(self.path.as_posix(), in_fd)

    def stat(self) -> FileInfo:
        return FileInfo(size=self.fs.getinfo(self.path.as_posix(), namespaces=['details']).size)

    def hexdigest(self, name: Literal["sha256", "md5"]) -> str:
        return self.fs.hash(self.path.as_posix(), name=name)
    
    @property
    def stem(self) -> str:
        return self.path.stem

    @property
    def suffix(self) -> str:
        """
        The final component's last suffix, if any.

        This includes the leading period. For example: '.txt'
        """
        return self.path.suffix

    def is_symlink(self, match_windows_shortcut: bool = True) -> bool:
        if match_windows_shortcut and self.path.suffix.lower() == '.lnk':
            return True
        return self.fs.getinfo(self.path.as_posix(), namespaces=['link']).is_link

    def readlink(self) -> Path:
        link_target = self.fs.getinfo(self.path.as_posix()).target
        assert link_target is not None, "Path is not a symlink"
        return Path(self.fs, link_target)

    def link(self, target: Path) -> None:
        if target.exists():
            return
        try:
            old_syspath = self.fs.getsyspath(self.path.as_posix())
            new_syspath = target.fs.getsyspath(target.path.as_posix())
            pathlib.Path(new_syspath).symlink_to(pathlib.Path(old_syspath))
        except fs.errors.NoSysPath:
            # Filesystem does not support syspaths; fall back on copying
            fs.copy.copy_file(self.fs, self.path.as_posix(), target.fs, target.path.as_posix())

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def parts(self) -> tuple[str, ...]:
        return self.path.parts

    def delete(self, *, allow_missing: bool = False) -> None:
        try:
            self.fs.remove(self.path.as_posix())
        except fs.errors.ResourceNotFound:
            if not allow_missing:
                raise

    def __hash__(self) -> int:
        return hash(self.path)
    
    def getsyspath(self) -> Optional[str]:
        """
        Get the file path on the local filesystem corresponding to this file.
        
        Returns None if the file is not accessible via a local file path.
        """
        try:
            return self.fs.getsyspath(self.path.as_posix())
        except fs.errors.NoSysPath:
            return None

    def children(self) -> Iterable[Path]:
        return (self / child for child in self.fs.listdir(self.path.as_posix()))


class Pipe:
    queue: asyncio.Queue[bytes]
    event_loop: asyncio.AbstractEventLoop
    done: asyncio.Event
    size: int

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.queue = asyncio.Queue()
        self.event_loop = loop
        self.done = asyncio.Event()
        self.size = 0

    async def read(self, size: int = -1) -> bytes:
        if size == -1:
            # This is a special case where the client is trying to read the entire file at once.
            data = BytesIO()
            while True:
                data_segment = await self.queue.get()
                if data_segment == b"":
                    break
                data.write(data_segment)
            return data.getvalue()
            
        return await self.queue.get()

    async def write(self, data: bytes) -> int:
        await self.queue.put(data)
        self.size += len(data)
        return len(data)

    @asynccontextmanager
    async def open(self) -> AsyncGenerator[ReadableStream, None]:
        yield self
    
class SyncPipeWriter:
    """
    A file-like object that can be written to synchronously.
    """
    pipe: Pipe

    def __init__(self, pipe: Pipe):
        self.pipe = pipe

    def write(self, s: SizedBuffer, /) -> int:
        return asyncio.run_coroutine_threadsafe(self.pipe.write(s), self.pipe.event_loop).result()

    def tell(self) -> int:
        return self.pipe.size
    
    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(self.async_close(), self.pipe.event_loop).result()

    async def async_close(self) -> None:
        await self.pipe.write(b"")
        self.pipe.done.set()


    def __enter__(self) -> "SyncPipeWriter":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    async def readinto(self, buffer: Buffer) -> int:
        raise NotImplementedError("readinto is not implemented for SyncPipeWriter")
