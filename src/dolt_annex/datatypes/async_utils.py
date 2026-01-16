#!/usr/bin/env python
# -*- coding: utf-8 -*-

from asyncio import Future
import inspect
from typing_extensions import Awaitable, Self

type MaybeAwaitable[T] = T | Awaitable[T]

async def maybe_await[U](v: MaybeAwaitable[U]) -> U:
    if inspect.isawaitable(v):
        return await v
    return v

class Result[T]:
    """
    The eventual result of a file operation.
    
    It wraps a Future that can be awaited on, but the result is not itself awaitable.
    This avoids ambiguity when MaybeAwaitable functions return Results.
    """

    future: Future[T]

    def __init__(self, future: Future[T]) -> None:
        self.future = future

    @classmethod
    def of(cls, value: T) -> Self:
        fut: Future[T] = Future()
        fut.set_result(value)
        return cls(fut)
    
    async def wait_for_complete(self) -> T:
        return await self.future

