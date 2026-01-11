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

from dataclasses import dataclass
import hashlib
import pathlib
from pydantic import InstanceOf
from typing_extensions import cast, override

from fs.base import FS as FileSystem
import fs.osfs

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.file_io import Path, ReadableFileObject
from dolt_annex.file_keys import FileKey
from dolt_annex.filestore.base import copy
from dolt_annex.filestore.file_handles import ExistingFileHandle

from .base import FileInfo, FileStore, FileStoreModel

@dataclass
class AnnexFS(FileStore):

    file_system: FileSystem

    @override
    async def put_file(self, file_path: Path, file_key: FileKey) -> None:
        """Move an on-disk file to the annex."""
        output_path = self.get_key_path(file_key)
        output_path.parent.mkdirs(exist_ok=True)
        file_path.rename(output_path)
        return

    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        """Copy a file-like object into the annex."""
        output_path = self.get_key_path(file_key)
        output_path.parent.mkdirs(exist_ok=True)
        with output_path.open('wb') as out_fd:
            await copy(src=in_fd, dst=out_fd)

    @override
    async def get_file_object(self, file_key: FileKey) -> ReadableFileObject:
        annexed_file_path = self.get_key_path(file_key)
        if not annexed_file_path.exists():
            # If the file does not exist at the expected path, try the deprecated path.
            annexed_file_path = self.get_old_key_path(file_key)
            if not annexed_file_path.exists():
                raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        fd = annexed_file_path.open()
        return ExistingFileHandle(readfile=fd, file_info=await self.stat(file_key))

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
        return self.get_key_path(file_key).stat()

    @override
    async def fstat(self, file_obj: ReadableFileObject) -> FileInfo:
        return cast(ExistingFileHandle, file_obj).file_info

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
    def exists(self, file_key: FileKey) -> bool:
        return self.get_key_path(file_key).exists()
    
class AnnexFSModel(FileStoreModel):
    root: pathlib.Path | InstanceOf[FileSystem]

    @override
    def create(self, config: Config) -> AnnexFS:
        if isinstance(self.root, pathlib.Path):
            self.root.mkdir(parents=True, exist_ok=True)
            file_system = fs.osfs.OSFS(str(self.root))
        else:
            file_system = self.root
        return AnnexFS(file_system=file_system)
