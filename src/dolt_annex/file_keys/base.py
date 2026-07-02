#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections.abc import Buffer, Set
import hashlib
from typing import Any, Callable, ClassVar, Dict, List, Type, overload, override
import parse
from typing_extensions import Optional, Protocol, Self

from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream, SizedBuffer
from dolt_annex.datatypes.file_io import Path

type FileKeyPrefix = bytes

class FileKeyMixinBase:
    @classmethod
    def normalize(cls, fields: Dict[str, Any]) -> Dict[str, Any]:
        return fields

class FileKey(FileKeyMixinBase):

    key: bytes

    prefixes: ClassVar[Dict[str, type[Self]]] = {}

    prefix: ClassVar[str]
    templates: ClassVar[List[str]]
    parsers: ClassVar[List[parse.Parser]]
    named_fields: ClassVar[Set[str]]
    hash_function: ClassVar[HashFunction]
    hash_name: ClassVar[str]

    def __init__(self, key: bytes):
        self.key = key
    
    @classmethod
    def try_parse(cls: type[Self], key: bytes) -> Optional[Self]:
        """Parse a key into a FileKey instance."""
        for prefix, subclass in cls.prefixes.items():
            if key.startswith(prefix.encode('utf-8')):
                return subclass(key)
        return None
    
    @overload
    @classmethod
    def must_parse(cls, key: bytes | str) -> FileKey:
        ...

    @overload
    @classmethod
    def must_parse(cls, key: None) -> None:
        ...

    @classmethod
    def must_parse(cls, key: Optional[bytes | str]) -> Optional[FileKey]:
        if key is None:
            return None
        if isinstance(key, str):
            key = key.encode('utf-8')
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
    
    def same_bytes(self, other: FileKey) -> bool:
        """Returns whether this FileKey is the same as another, ignoring extensions."""
        return self.remove_extension() == other.remove_extension()
    
    def __getitem__(self, field_name: str) -> Optional[str]:
        """Returns the value of the specified field, if any."""
        return self.to_fields().get(field_name)

    @classmethod
    def convert_from(cls, other: FileKey) -> Self:
        return cls.from_fields(**other.to_fields())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FileKey):
            return NotImplemented
        return self.key == other.key
    
    def __repr__(self) -> str:
        return str(self)

    def __init_subclass__(
        cls,
        *,
        prefix: str, 
        templates: List[str],
        hash_function: HashFunction,
        hash_name: str
    ) -> None:
        cls.prefixes[prefix] = cls
        cls.prefix = prefix
        cls.templates = templates
        cls.parsers = [parse.compile(template, case_sensitive=True) for template in templates]
        cls.named_fields = {field for parser in cls.parsers for field in parser.named_fields}
        cls.hash_function = staticmethod(hash_function)
        cls.hash_name = hash_name

        @classmethod
        def try_parse(cls: type[Self], key: bytes) -> Optional[Self]:
            if key.startswith(cls.prefix.encode('utf-8')):
                return cls(key)
            return None
        cls.try_parse = try_parse

    @classmethod
    async def from_file(cls, file_path: Path, extension: Optional[str] = None) -> Self:
        """Generate a FileKey from a file on disk."""
        if extension is None:
            extension = file_path.suffix[1:]
        async with file_path.open() as fd:
            return await cls.from_fo(fd, extension=extension)

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
    def generator(cls, extension: Optional[str] = None) -> FileKeyGenerator[Self]:
        """Return a FileKeyGenerator for this FileKey type."""
        return FileKeyGenerator[Self](fileKeyType=cls, extension=extension)

    @classmethod
    def convertable_from(cls, other: type[FileKey]) -> bool:
        return cls.named_fields <= other.named_fields
    
    def remove_extension(self) -> Self:
        """Returns a file key with extension removed, if any."""
        fields = self.to_fields()
        if "extension" in fields:
            del fields["extension"]
        return self.__class__.from_fields(**fields)
    
    def to_fields(self) -> Dict[str, Any]:
        for parser in self.parsers:
            parse_results = parser.parse(str(self))
            if parse_results is not None:
                return parse_results.named
        raise ValueError(f"Could not parse file key: {self.key!r}")
    
    @classmethod
    def from_fields(cls, **fields: Dict[str, Any]) -> Self:
        fields = cls.normalize(fields)
        for template in cls.templates:
            try:
                key_str = template.format_map(fields)
                return cls(key_str.encode('utf-8'))
            except KeyError:
                continue
        raise ValueError(f"Could not create file key from fields: {fields!r}")
    
class FileKeyGenerator[FileKeyType: FileKey = FileKey]:
    
    _fileKeyType: type[FileKeyType]
    _hasher: HasherProtocol
    _size: int
    _extension: Optional[str]
    
    def __init__(self, fileKeyType: Type[FileKeyType], extension: Optional[str] = None) -> None:
        self._fileKeyType = fileKeyType
        self._hasher = fileKeyType.hash_function()
        self._size = 0
        self._extension = extension

    def update(self, data: SizedBuffer) -> None:
        self._hasher.update(data)
        self._size += len(data)

    def finalize(self) -> FileKeyType:
        hash_string = self._hasher.hexdigest()
        fields = {
            "size": self._size,
            self._fileKeyType.hash_name: hash_string
        }
        if self._extension:
            fields["extension"] = self._extension
        return self._fileKeyType.from_fields(**fields)

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

class HasherProtocol(Protocol):

    def update(self, data: Buffer, /) -> None:
        ...
    
    def hexdigest(self) -> str:
        ...

type HashFunction = Callable[[], HasherProtocol]

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
    
class LowercaseExtensionMixin(FileKeyMixinBase):
    @classmethod
    @override
    def normalize(cls, fields: Dict[str, Any]) -> Dict[str, Any]:
        result = super().normalize(fields).copy()
        if "extension" in result and result["extension"] is not None:
            result["extension"] = result["extension"].lower()
        return result

class Sha256HSe(
    FileKey,
    LowercaseExtensionMixin,
    prefix="SHA256_HSe",
    templates=["SHA256_HSe-{sha256:64}--s{size}.{extension}", "SHA256_HSe-{sha256:64}--s{size}"],
    hash_function=hashlib.sha256,
    hash_name="sha256"
):
    """
    SHA256_HSe file keys have the format: SHA256_HSe-<sha256>--s<size>.<extension>
    
    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

class MD5HSe(
    FileKey,
    LowercaseExtensionMixin,
    prefix="MD5_HSe",
    templates=["MD5_HSe-{md5:32}--s{size}.{extension}", "MD5_HSe-{md5:32}--s{size}"],
    hash_function=hashlib.md5,
    hash_name="md5"
):
    """
    MD5_HSe file keys have the format: MD5_HSe-<md5>--s<size>.<extension>
    
    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

class Sha1HSe(
    FileKey,
    LowercaseExtensionMixin,
    prefix="SHA1_HSe",
    templates=["SHA1_HSe-{sha1:40}--s{size}.{extension}", "SHA1_HSe-{sha1:40}--s{size}"],
    hash_function=hashlib.sha1,
    hash_name="sha1"
):
    """
    SHA1_HSe file keys have the format: SHA1_HSe-<sha1>--s<size>.<extension>
    
    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

class Sha256E(
    FileKey,
    prefix="SHA256E",
    templates=["SHA256E-s{size}--{sha256}.{extension}", "SHA256E-s{size}--{sha256}"],
    hash_function=hashlib.sha256,
    hash_name="sha256"
):
    """
    SHA256E file keys have the format: 
    
    Note the uppercase "E" in the prefix. This indicates that the extension is not lowercased.
    
    Our original implementation did not lowercase the extension for SHA256E keys.
    We are preserving this behavior for backward compatibility.
    """

class MD5e(
    FileKey,
    LowercaseExtensionMixin,
    prefix="MD5e",
    templates=["MD5e-s{size}--{md5:32}.{extension}", "MD5e-s{size}--{md5:32}"],
    hash_function=hashlib.md5,
    hash_name="md5"
):
    """
    MD5e file keys have the format: MD5e-s{size}--{md5}.{extension.lower()}

    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

class SHA1e(
    FileKey,
    LowercaseExtensionMixin,
    prefix="SHA1e",
    templates=["SHA1e-s{size}--{sha1:40}.{extension}", "SHA1e-s{size}--{sha1:40}"],
    hash_function=hashlib.sha1,
    hash_name="sha1"
):
    """
    SHA1e file keys have the format: SHA1e-s{size}--{sha1}.{extension.lower()}
    
    Note the lowercase "e" in the prefix. This indicates that the extension is lowercased.
    """

