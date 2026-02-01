#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import ClassVar, Dict
from typing_extensions import Optional, Self

from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream
from dolt_annex.datatypes.file_io import Path

@dataclass
class FileKey:
    """
    A key used to identify a file in the filestore.

    Each subclass describes a specific file key format.
    """

    prefixes: ClassVar[Dict[str, type[Self]]] = {}

    def __init_subclass__(cls, prefix: str) -> None:
        cls.prefixes[prefix] = cls

    key: bytes

    @classmethod
    async def from_file(cls, file_path: Path, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file on disk."""
        async with file_path.open() as fd:
            return await cls.from_fo(fd, extension=extension)

    @classmethod
    async def from_fo(cls, file_obj: ReadableFileObject, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file-like object."""
        file_bytes = await file_obj.read()
        await file_obj.seek(0)
        return cls.from_bytes(file_bytes, extension=extension)

    @classmethod
    @abstractmethod
    def from_bytes(cls, file_bytes: bytes, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from bytes in memory."""
        raise NotImplementedError()

    @classmethod
    def try_parse(cls, key: bytes) -> Optional[Self]:
        """Parse a key into a FileKey instance."""
        for prefix, subclass in cls.prefixes.items():
            if key.startswith(prefix.encode('utf-8')):
                return subclass.try_parse(key)
        return None
    
    @classmethod
    def must_parse(cls, key: bytes) -> Self:
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
    
    def size(self) -> int:
        """Return the size of the file represented by this key, if known."""
        raise NotImplementedError()