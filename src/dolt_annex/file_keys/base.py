#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from typing_extensions import Optional, Self

@dataclass
class FileKey:
    """
    A key used to identify a file in the filestore.

    Each subclass describes a specific file key format.
    """

    key: bytes

    @classmethod
    def from_file(cls, file_path: Path, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file on disk."""
        with open(file_path, 'rb') as fd:
            return cls.from_fo(fd, extension=extension)

    @classmethod
    def from_fo(cls, file_obj: BinaryIO, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file-like object."""
        file_bytes = file_obj.read()
        file_obj.seek(0)
        return cls.from_bytes(file_bytes, extension=extension)

    @classmethod
    @abstractmethod
    def from_bytes(cls, file_bytes: bytes, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from bytes in memory."""

    @classmethod
    @abstractmethod
    def try_parse(cls, key: bytes) -> Optional[Self]:
        """Validate a key."""

    def __bytes__(self) -> bytes:
        return self.key

    def __str__(self) -> str:
        return self.key.decode('utf-8')

    def __hash__(self) -> int:
        return hash(self.key)
