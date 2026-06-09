#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from abc import abstractmethod
import copy
import hashlib
from typing import ClassVar, Type, override
from typing_extensions import Optional, Self

from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream
from dolt_annex.file_keys.base import FileKey, FileKeyGenerator, HasherProtocol

class HashSizeExtensionFileKey(FileKey, is_abstract=True):
    """
    An abstract parent class for FileKeys that uses the format: <preifx>-<hash>--s<size>[.<extension>]
    """

    lower_extensions: ClassVar[bool]

    def __init_subclass__(cls, *, prefix: str, lower_extensions: bool) -> None:
        cls.lower_extensions = lower_extensions
        super().__init_subclass__(prefix=prefix)

    key: bytes
    _hash_string: Optional[str]
    _file_size: Optional[int]

    def __init__(self, *, key: bytes, hash_string: Optional[str] = None, file_size: Optional[int] = None) -> None:
        super().__init__(key=key)
        self._hash_string = hash_string
        self._file_size = file_size

    @property
    def hash(self) -> str:
        """Return the hash string of the file represented by this key."""
        if self._hash_string is None:
            self._hash_string = self.key.split(b'--')[1].split(b'.')[0].decode('utf-8')
        return self._hash_string

    @property
    def size(self) -> int:
        """Return the size of the file represented by this key."""
        if self._file_size is None:
            size_str = self.key.split(b'--')[0].split(b'-s')[1]
            self._file_size = int(size_str)
        return self._file_size
    
    @property
    def extension(self) -> Optional[str]:
        parts = self.key.rsplit(b'.', 1)
        if len(parts) == 1:
            return None
        return parts[1].decode("utf-8")

    @classmethod
    async def from_fo(cls, file_obj: ReadableFileObject, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file-like object."""
        generator = cls.generator(extension=extension)
        await file_digest(file_obj, generator)
        return generator.finalize()

    @classmethod
    def from_bytes(cls, file_bytes: bytes, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from bytes in memory."""
        generator = cls.generator(extension=extension)
        generator.update(file_bytes)
        return generator.finalize()
    
    @classmethod
    @override
    def try_parse(cls, key: bytes) -> Optional[Self]:
        """Generate a FileKey from bytes in memory."""
        if key.startswith(cls.prefix.encode('utf-8')):
            return cls(key=key)
        return None
    
    @classmethod
    def generator(cls, extension: Optional[str] = None) -> Generator[Self]:
        """Return a FileKeyGenerator for this FileKey type."""
        return Generator[Self](fileKeyType=cls, extension=extension)
    
    @override
    def remove_extension(self) -> Self:
        """Returns a file key with extension removed, if any."""
        base_key = self.key.split(b'.')[0]
        return copy.replace(self, key=base_key)
    
    @classmethod
    @abstractmethod
    def hash_function(cls) -> HasherProtocol:
        """Return the hash function used by this FileKey type."""

    @classmethod
    def make(cls, size: int, hash_string: str, extension: Optional[str] = None) -> Self:
        """Create a FileKey from its components."""
        key=f"{cls.prefix}-{hash_string}--s{size}".encode('utf-8')
        if extension:
            if cls.lower_extensions:
                extension = extension.lower()
            key += f".{extension}".encode('utf-8')
        return cls(key=key, hash_string=hash_string, file_size=size)

class Generator[T: HashSizeExtensionFileKey](FileKeyGenerator):
    
    def __init__(self, fileKeyType: Type[T], extension: Optional[str] = None) -> None:
        self._fileKeyType = fileKeyType
        self._hasher = fileKeyType.hash_function()
        self._size = 0
        self._extension = extension
        hashlib.sha256()

    @override
    def update(self, data: bytes) -> None:
        self._hasher.update(data)
        self._size += len(data)

    @override
    def finalize(self) -> T:
        hash_string = self._hasher.hexdigest()
        return self._fileKeyType.make(self._size, hash_string, self._extension)


async def file_digest(fileobj: ReadableStream, digest: FileKeyGenerator, /, *, _bufsize=2**18):
    if hasattr(fileobj, "getbuffer"):
        digest.update(fileobj.getbuffer()) # type: ignore
        return

    if hasattr(fileobj, "readinto"):
        buf = bytearray(_bufsize)
        view = memoryview(buf)
        while True:
            size = await fileobj.readinto(buf)
            if size == 0:
                break
            digest.update(view[:size])
    else:
        while True:
            buf = await fileobj.read(_bufsize)
            if not buf:
                break
            digest.update(buf)

    return

class Sha256HSe(HashSizeExtensionFileKey, prefix="SHA256_HSe", lower_extensions=True):
    """
    SHA256_HSe file keys have the format: SHA256_HSe-<sha256>--s<size>.<extension>
    
    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

    @override
    @classmethod
    def hash_function(cls) -> hashlib.HASH:
        return hashlib.sha256()

class MD5HSe(HashSizeExtensionFileKey, prefix="MD5_HSe", lower_extensions=True):
    """
    MD5_HSe file keys have the format: MD5_HSe-{md5}--s{size}.{extension.lower()}

    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

    @override
    @classmethod
    def hash_function(cls) -> hashlib.HASH:
        return hashlib.md5()
    
class Sha1HSe(HashSizeExtensionFileKey, prefix="SHA1_HSe", lower_extensions=True):
    """
    SHA1_HSe file keys have the format: SHA1_HSe-{sha1}--s{size}.{extension.lower()}
    
    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

    @override
    @classmethod
    def hash_function(cls) -> hashlib.HASH:
        return hashlib.sha1()
