from __future__ import annotations

import contextvars
from dataclasses import dataclass
import pathlib

from types import TracebackType
from typing import BinaryIO, Protocol, Self, cast
from typing_extensions import Buffer, Literal

import fs.move
from fs.base import FS
from fs.osfs import OSFS

from dolt_annex.datatypes.async_utils import MaybeAwaitable

@dataclass
class FileInfo:
    size: int | None

class BaseFileObject(Protocol):
    def close(self) -> MaybeAwaitable[None]:
        ...

    def __enter__(self) -> Self:
        ...

    def __exit__(self, type: type[BaseException] | None, value: BaseException | None, traceback: TracebackType | None, /) -> None:
        ...

class ReadableFileObject(BaseFileObject, Protocol):
    def read(self, size: int = -1, /) -> MaybeAwaitable[bytes]:
        ...

class WritableFileObject(BaseFileObject, Protocol):
    def write(self, s: Buffer, /) -> MaybeAwaitable[int]:
        ...

class FileObject(ReadableFileObject, WritableFileObject, Protocol):
    pass

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
    
    def open(self, mode: Literal['rb', 'wb'] = 'rb') -> BinaryIO:
        return cast(BinaryIO, self.fs.open(self.path.as_posix(), mode))

    @property
    def parent(self) -> Path:
        return Path(self.fs, self.path.parent)

    def mkdirs(self, exist_ok: bool = False) -> None:
        self.fs.makedirs(self.path.as_posix(), recreate=exist_ok)

    def rename(self, target: Path) -> None:
        fs.move.move_file(self.fs, self.path.as_posix(), target.fs, target.path.as_posix())

    def upload(self, in_fd: ReadableFileObject) -> None:
        self.fs.upload(self.path.as_posix(), in_fd)

    def stat(self) -> FileInfo:
        return FileInfo(size=self.fs.getinfo(self.path.as_posix(), namespaces=['details']).size)
    
    def hexdigest(self, name: Literal["sha256", "md5"]) -> str:
        return self.fs.hash(self.path.as_posix(), name=name)
    
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
        return self.fs.getinfo(self.path.as_posix()).is_link
    
    def readlink(self) -> Path:
        link_target = self.fs.getinfo(self.path.as_posix()).target
        assert link_target is not None, "Path is not a symlink"
        return Path(self.fs, link_target)
    
    @property
    def name(self) -> str:
        return self.path.name
    
    def parts(self) -> tuple[str, ...]:
        return self.path.parts