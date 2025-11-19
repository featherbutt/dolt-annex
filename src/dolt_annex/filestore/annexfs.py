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
from io import BufferedWriter
import os
from pathlib import Path
from typing_extensions import override

from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileObject, FileStore, copy

class AnnexFS(FileStore):

    root: Path

    @override
    async def put_file(self, file_path: Path, file_key: FileKey) -> None:
        """Move an on-disk file to the annex."""
        output_path = self.get_key_path(file_key)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.rename(output_path)
        return

    @override
    async def put_file_object(self, in_fd: FileObject, file_key: FileKey) -> None:
        """Copy a file-like object into the annex."""
        output_path = self.get_key_path(file_key)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out_fd: BufferedWriter
        print(f"Writing file to {output_path}")
        with open(output_path, 'wb') as out_fd:
            await copy(src=in_fd, dst=out_fd)

    @override
    async def get_file_object(self, file_key: FileKey) -> FileObject:
        annexed_file_path = self.get_key_path(file_key)
        if not annexed_file_path.exists():
            # If the file does not exist at the expected path, try the deprecated path.
            annexed_file_path = self.get_old_key_path(file_key)
            if not annexed_file_path.exists():
                raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        fd = open(annexed_file_path, 'rb')
        return fd
    
    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
         return FileInfo(size=self.get_key_path(file_key).stat().st_size)

    @override
    async def fstat(self, file_obj: FileObject) -> FileInfo:
         return FileInfo(size=os.fstat(file_obj.fileno()).st_size)

    def get_key_path(self, key: FileKey) -> Path:
        """
        Get the relative path for a file in the annex from its key.
        """
        md5 = hashlib.md5(bytes(key)).hexdigest()
        return self.root / md5[:3] / md5[3:6] / str(key)

    def get_old_key_path(self, key: FileKey) -> Path:
        """
        Get the relative path for an annex key using the old layout that includes an extra directory
        with the same name as the key.

        Some older versions of dolt-annex used this layout, so we fall back to it when looking for files.
        """
        md5 = hashlib.md5(bytes(key)).hexdigest()
        return self.root / md5[:3] / md5[3:6] / str(key) / str(key)

    def get_absolute_file_path(self, path: Path) -> Path:
        if path.is_absolute():
            return path
        return (self.root / path).resolve()

    @override
    def exists(self, file_key: FileKey) -> bool:
        return self.get_key_path(file_key).exists()