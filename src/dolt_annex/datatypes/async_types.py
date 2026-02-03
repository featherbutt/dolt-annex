#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module defines various types and protocols for async io.

Definitions here should not depend on any other dolt_annex modules.
"""

from collections.abc import Awaitable, Buffer
import inspect
import os
from types import TracebackType
from typing import Any
from typing_extensions import Protocol

type MaybeAwaitable[T] = T | Awaitable[T]

async def maybe_await[U](v: MaybeAwaitable[U]) -> U:
    """
    Unwrap a value if it is awaitable, otherwise return it directly.
    
    This allows us to implement interfaces that can be either sync or async.
    """
    if inspect.isawaitable(v):
        return await v
    return v

# The following protocols describe various file-like objects with different capabilities.
# Since different filestores have different requirements,
# these protocols allow us to use the many different filestores in a type-safe way.

class AsyncContextManager[T](Protocol):
    async def __aenter__(self) -> T: ...
    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None, /) -> Any: ...

class Closable(Protocol):
    def close(self) -> Awaitable[None]:
        ...

class AwaitOrEnter[T: Closable](Awaitable[T], AsyncContextManager, Protocol):
    """
    This type can be used in two ways:
    1. As an async context manager, which will open and close the resource.
    2. As an awaitable, which will open the resource, but the caller is responsible for closing it.

    The first usage is preferred, but if the resource is long-lived, the second usage may be more convenient.
    """

class ReadableStream(Closable, Protocol):
    def read(self, size: int = -1, /) -> Awaitable[bytes]:
        ...

    def readinto(self, b: Buffer, /) -> Awaitable[int]:
        ...

class WritableStream(Closable, Protocol):
    def write(self, s: Buffer, /) -> Awaitable[int]:
        ...

class ReadableFileObject(ReadableStream, Protocol):
    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> Awaitable[int]:
        ...

    def tell(self) -> Awaitable[int]:
        ...

class WritableFileObject(WritableStream, ReadableFileObject, Protocol):
    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> Awaitable[int]:
        ...

    def tell(self) -> Awaitable[int]:
        ...
