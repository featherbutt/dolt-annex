#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
from typing_extensions import Optional, Self, override

from dolt_annex.datatypes.file_io import Path

from .base import FileKey, FileKeyGenerator

class Sha256e(FileKey, prefix="SHA256E-"):
    """SHA256e file keys have the format: SHA256E-s<size>--<sha256>.<extension>"""

    @classmethod
    def make(cls, size: int, sha256: str, extension: Optional[str] = None):
        if extension:
            return cls(
                key=b"SHA256E-s%s--%s.%s" % (str(size).encode('utf-8'), sha256.encode('utf-8'), extension.encode('utf-8'))
            )
        else:
            return cls(
                key=b"SHA256E-s%s--%s" % (str(size).encode('utf-8'), sha256.encode('utf-8'))
            )

    @classmethod
    @override
    async def from_file(cls, file_path: Path, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from the hash of a file."""
        if extension is None:
            extension = file_path.suffix[1:].lower() or None
        async with file_path.open('rb') as f:
            data = await f.read()
        return cls.from_bytes(data, extension)

    @classmethod
    @override
    def from_bytes(cls, file_bytes: bytes, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from bytes in memory."""
        data_hash = hashlib.sha256(file_bytes).hexdigest()
        return cls.make(len(file_bytes), data_hash, extension)
    
    @classmethod
    @override
    def try_parse(cls, key: bytes) -> Optional[Self]:
        """Generate a FileKey from bytes in memory."""
        if key.startswith(b"SHA256E-s"):
            return cls(key=key)
        return None

    @override
    def size(self) -> int:
        """Return the size of the file represented by this key, if known."""
        size_str = self.key.split(b'--')[0].split(b'-s')[1]
        return int(size_str)
    
    @override
    @classmethod
    def generator(cls, extension: Optional[str] = None) -> FileKeyGenerator:
        """Return a FileKeyGenerator for this FileKey type."""
        return Sha256eGenerator(extension=extension)
    
    @override
    def same_bytes(self, other: FileKey) -> bool:
        """Return whether this FileKey represents the same file as another FileKey, ignoring extensions."""
        return (
            isinstance(other, Sha256e) and
            self.key.split(b'.')[0] == other.key.split(b'.')[0]
        )
    
class Sha256eGenerator(FileKeyGenerator):
    
    def __init__(self, extension: Optional[str] = None) -> None:
        self._hasher = hashlib.sha256()
        self._size = 0
        self._extension = extension

    @override
    def append_data(self, data: bytes) -> None:
        self._hasher.update(data)
        self._size += len(data)

    @override
    def finalize(self) -> FileKey:
        sha256 = self._hasher.hexdigest()
        return Sha256e.make(self._size, sha256, self._extension)