#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
from pathlib import Path
from typing_extensions import Optional, Self, override

from .base import FileKey

class Sha256e(FileKey):
    """SHA256e file keys have the format: SHA256E-s<size>--<sha256>.<extension>"""

    @classmethod
    def make(cls, size: int, sha256: str, extension: Optional[str] = None):
        if extension:
            return cls(b"SHA256E-s%s--%s.%s" % (str(size).encode('utf-8'), sha256.encode('utf-8'), extension.encode('utf-8')))
        else:
            return cls(b"SHA256E-s%s--%s" % (str(size).encode('utf-8'), sha256.encode('utf-8')))

    @classmethod
    @override
    def from_file(cls, file_path: Path, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from the hash of a file."""
        if extension is None:
            extension = file_path.suffix[1:].lower() or None
        with open(file_path, 'rb') as f:
            data = f.read()
        return cls.from_bytes(data, extension)

    @classmethod
    @override
    def from_bytes(cls, file_bytes: bytes, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from bytes in memory."""
        data_hash = hashlib.sha256(file_bytes).hexdigest()
        return cls.make(len(file_bytes), data_hash, extension)
