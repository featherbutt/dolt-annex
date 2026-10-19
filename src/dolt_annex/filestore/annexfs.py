#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AnnexFS is a filestore type that stores every file in the dataset as a separate
file on disk, sharded into a directory structure based on git-annex's file
layout and the md5 hash of the annex key.

For example, the file key
`SHA256E-s5--2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.txt`
has an md5 hash beginning with `091de9...`, and is thus found at `./091/de9/`
relative to the filestore root.
"""

import hashlib
import pathlib
import random
from typing import AsyncGenerator
from pydantic import InstanceOf
from typing_extensions import override

from fs.base import FS as FileSystem
import fs.osfs

from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream
from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.config.base import Config
from dolt_annex.datatypes.file_io import Path
from dolt_annex.file_keys import FileKey
from dolt_annex.filestore.base import copy
from dolt_annex.filestore.file_handles import ExistingFileHandle
from dolt_annex.datatypes.file_stream import KeyedFileStream

from .base import FileInfo, FileStore, FileStoreModel

class AnnexFS(FileStore):

    file_system: FileSystem

    def __init__(self, model: AnnexFSModel, file_system: FileSystem):
        super().__init__(model)
        self.file_system = file_system

    @override
    async def put_file_object(self, data_source: KeyedFileStream, overwrite_existing: bool = False) -> FileStore.PutFileResult:
        """Copy a file-like object into the annex."""
        output_path = Path(self.file_system) / "tmp" / random.randbytes(16).hex()
        output_path.parent.mkdirs(exist_ok=True)
        async with (
            output_path.open('wb') as out_fd,
            data_source as in_fd,
        ):
            await copy(src=in_fd, dst=out_fd)
        file_key = await data_source.file_key()
        # TODO: Handle the case where the file already exists.
        output_path.rename(self.get_key_path(file_key))
        return FileStore.PutFileResult(file_key=file_key, uuids=[self.uuid] if self.uuid else [])

    @override
    @await_or_enter
    async def with_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        annexed_file_path = self.get_key_path(file_key)
        if not annexed_file_path.exists() or annexed_file_path.is_dir():
            # If the file does not exist at the expected path, try the deprecated path.
            annexed_file_path = self.get_old_key_path(file_key)
            if not annexed_file_path.exists():
                raise FileNotFoundError(f"File with key {file_key} not found in annex.")

        async with annexed_file_path.open() as fd:
            yield ExistingFileHandle(readfile=fd, file_info=await self.stat(file_key))
        

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        return self.get_key_path(file_key).stat()

    @override
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, ExistingFileHandle):
            raise TypeError("AnnexFS.fstat was passed a file object that did not originate from this filestore.")
        return file_obj.file_info

    def get_key_path(self, key: FileKey) -> Path:
        """
        Get the relative path for a file in the annex from its key.
        """
        md5 = hashlib.md5(bytes(key)).hexdigest()
        return Path(self.file_system) / md5[:3] / md5[3:6] / str(key)

    def get_old_key_path(self, key: FileKey) -> Path:
        """
        Get the relative path for an annex key using the old layout that includes an extra directory
        with the same name as the key.

        Some older versions of dolt-annex used this layout, so we fall back to it when looking for files.
        """
        md5 = hashlib.md5(bytes(key)).hexdigest()
        return Path(self.file_system) / md5[:3] / md5[3:6] / str(key) / str(key)

    @override
    async def exists(self, file_key: FileKey) -> bool:
        return self.get_key_path(file_key).exists()

    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey):
        """
        Insert a new key that references the same content as an existing key.
        """
        if old_key == new_key:
            return
        old_path = self.get_key_path(old_key)
        new_path = self.get_key_path(new_key)
        new_path.parent.mkdirs(exist_ok=True)
        old_path.link(new_path)
    
    @override
    def delete(self, key: FileKey):
        self.get_old_key_path(key).delete(allow_missing=True)
        self.get_key_path(key).delete(allow_missing=True)

class AnnexFSModel(FileStoreModel):
    root: pathlib.Path | InstanceOf[FileSystem]

    @override
    def create(self, config: Config) -> AnnexFS:
        if isinstance(self.root, pathlib.Path):
            self.root.mkdir(parents=True, exist_ok=True)
            file_system = fs.osfs.OSFS(str(self.root))
        else:
            file_system = self.root
        return AnnexFS(model=self, file_system=file_system)
