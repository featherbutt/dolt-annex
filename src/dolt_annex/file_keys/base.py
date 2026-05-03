#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Buffer
from dataclasses import dataclass
from typing import ClassVar, Dict, overload
from typing_extensions import Literal, Optional, Protocol, Self

from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream
from dolt_annex.datatypes.file_io import Path

type FileKeyPrefix = bytes

@dataclass
class FileKey:
    """
    A key used to identify a file in the filestore.

    Each subclass describes a specific file key format.
    """

    prefixes: ClassVar[Dict[str, type[Self]]] = {}
    prefix: ClassVar[str]

    @overload
    def __init_subclass__(cls, *, prefix: str) -> None:
        ...

    @overload
    def __init_subclass__(cls, *, is_abstract: Literal[True]) -> None:
        ...

    def __init_subclass__(cls, **kwargs) -> None:
        is_abstract: bool = kwargs.pop('is_abstract', False)
        if not is_abstract:
            prefix: str = kwargs.pop('prefix', None)
            cls.prefixes[prefix] = cls
            cls.prefix = prefix

    key: bytes

    @classmethod
    async def from_file(cls, file_path: Path, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file on disk."""
        if extension is None:
            extension = file_path.suffix[1:]
        async with file_path.open() as fd:
            return await cls.from_fo(fd, extension=extension)

    @classmethod
    @abstractmethod
    async def from_fo(cls, file_obj: ReadableFileObject, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file-like object."""

    @classmethod
    @abstractmethod
    def from_bytes(cls, file_bytes: bytes, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from bytes in memory."""

    @classmethod
    @abstractmethod
    def try_parse(cls, key: bytes) -> Optional[Self]:
        """Parse a key into a FileKey instance."""
        for prefix, subclass in cls.prefixes.items():
            if key.startswith(prefix.encode('utf-8')):
                return subclass.try_parse(key)
        return None
    
    @classmethod
    def must_parse(cls, key: bytes | str) -> Self:
        if isinstance(key, str):
            key = bytes(key, encoding='utf-8')
        file_key = cls.try_parse(key)
        if file_key is None:
            raise ValueError(f"Could not parse file key: {key!r}")
        return file_key

    def __bytes__(self) -> bytes:
        return self.key

    def __str__(self) -> str:
        return self.key.decode('utf-8')

    def __hash__(self) -> int:
        return hash(self.key)
    
    @classmethod
    @abstractmethod
    def generator(cls, extension: Optional[str] = None) -> FileKeyGenerator:
        """Return a FileKeyGenerator for this FileKey type."""
    
    @abstractmethod
    def remove_extension(self) -> Self:
        """Returns a file key with extension removed, if any."""

    def same_bytes(self, other: FileKey) -> bool:
        """Returns whether this FileKey is the same as another, ignoring extensions."""
        return self.remove_extension() == other.remove_extension()
    
    @property
    @abstractmethod
    def extension(self) -> Optional[str]:
        """Returns the file extension of this FileKey, if any."""
    
class FileKeyGenerator(Protocol):
    
    @abstractmethod
    def update(self, data: bytes) -> None:
        ...

    @abstractmethod
    def finalize(self) -> FileKey:
        ...

class HasherProtocol(Protocol):

    def update(self, data: bytes) -> None:
        ...
    
    def hexdigest(self) -> str:
        ...

class FileKeyGeneratingReader(ReadableStream):
    """
    A ReadableStream that wraps another ReadableStream
    and computes hash-based FileKeys as data is read.
    """

    _generators: list[FileKeyGenerator]
    _inner: ReadableStream

    def __init__(self, inner: ReadableStream, generators: list[FileKeyGenerator]) -> None:
        self._inner = inner
        self._generators = generators

    async def read(self, size: int = -1) -> bytes:
        if size == -1:
            data = await self._inner.read()
        else:
            data = await self._inner.read(size)
        for generator in self._generators:
            generator.update(data)
        return data
    
    async def readinto(self, buffer: Buffer) -> int:
        size = await self._inner.readinto(buffer)
        if size > 0:
            data = memoryview(buffer)[:size]
            for generator in self._generators:
                generator.update(data)
        return size

    def get_file_keys(self) -> list[FileKey]:
        return [generator.finalize() for generator in self._generators]
    
    async def close(self) -> None:
        return await self._inner.close()