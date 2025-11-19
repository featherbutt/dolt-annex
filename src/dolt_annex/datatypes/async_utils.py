import inspect
from typing_extensions import Awaitable

type MaybeAwaitable[T] = T | Awaitable[T]

async def maybe_await[U](v: MaybeAwaitable[U]) -> U:
    if inspect.isawaitable(v):
        return await v
    return v
