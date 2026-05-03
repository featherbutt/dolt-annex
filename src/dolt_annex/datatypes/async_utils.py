#!/usr/bin/env python
# -*- coding: utf-8 -*-

from asyncio import Future, sleep
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from typing_extensions import Self

from dolt_annex.datatypes.async_types import AsyncContextManager, AwaitOrEnter, Closable, ContextManager, MaybeAwaitable, maybe_await

class Result[T]:
    """
    The eventual result of a file operation.
    
    It wraps a Future that can be awaited on, but the result is not itself awaitable.
    This avoids ambiguity when MaybeAwaitable functions return Results.
    """

    future: Awaitable[T]

    def __init__(self, future: Awaitable[T]) -> None:
        self.future = future

    @classmethod
    def of(cls, value: T) -> Self:
        fut: Future[T] = Future()
        fut.set_result(value)
        return cls(fut)
    
    @staticmethod
    def done() -> 'Result[None]':
        return Result.of(None)
    
    async def wait_for_complete(self) -> T:
        return await self.future
    
    def map[S](self, func: 'Callable[[T], MaybeAwaitable[S | Result[S]]]') -> 'Result[S]':

        async def _map() -> S:
            result = await maybe_await(func(await self.wait_for_complete()))
            if isinstance(result, Result):
                return await result.wait_for_complete()
            else:
                return result

        return Result(_map())
    
    def and_then[S](self, func: 'Callable[[], MaybeAwaitable[S | Result[S]]]') -> 'Result[S]':
        return self.map(lambda _: func())

def await_or_enter[T: Closable, **P](coro: Callable[P, AsyncGenerator[T, None]]) -> Callable[P, AwaitOrEnter[T]]:
    """
    A decorator that converts an async generator function into an object that can be either awaited on
    or used as an async context manager.
    """
    acm = asynccontextmanager(coro)
    def inner(*args: P.args, **params: P.kwargs) -> AwaitOrEnter[T]:
        return AwaitOrEnterWrapper(acm(*args, **params))
    return inner

class AwaitOrEnterWrapper[T: Closable](AwaitOrEnter[T]):
    """
    A wrapper around an AsyncContextManager that allows an alternative usage pattern
    where the resource is explicitly closed.
    """

    __slots__ = ("_context", "_val")

    def __init__(self, context: AsyncContextManager[T]) -> None:
        self._context = context
        self._val: T | None = None

    def __await__(self):
        if self._val is None:
            self._val = yield from self._context.__aenter__().__await__()
        return self._val

    async def __aenter__(self) -> T:
        self._val = await self._context.__aenter__()
        return self._val

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._context.__aexit__(exc_type, exc_val, exc_tb)
        self._val = None

    async def close(self) -> None:
        if self._val is not None:
            await self._val.close()
            self._val = None

@asynccontextmanager
async def as_acm[T](sync_cm: ContextManager[T]):
    with sync_cm as result:
        await sleep(0)
        yield result
