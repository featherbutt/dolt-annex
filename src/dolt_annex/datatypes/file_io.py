from dataclasses import dataclass
from typing import Protocol
from typing_extensions import Buffer
from dolt_annex.datatypes.async_utils import MaybeAwaitable

@dataclass
class FileInfo:
    size: int | None

class ReadableFileObject(Protocol):
    def close(self) -> MaybeAwaitable[None]:
        ...

    def read(self, size: int = -1, /) -> MaybeAwaitable[bytes]:
        ...

class WritableFileObject(Protocol):
    def close(self) -> MaybeAwaitable[None]:
        ...

    def write(self, s: Buffer, /) -> MaybeAwaitable[int]:
        ...

class FileObject(ReadableFileObject, WritableFileObject, Protocol):
    pass