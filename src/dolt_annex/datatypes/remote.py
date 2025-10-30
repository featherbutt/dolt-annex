#!/usr/bin/env python
# -*- coding: utf-8 -*-

import getpass
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel

from dolt_annex.file_keys import FileKeyType
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.sftp import SftpFileStore

from .loader import Loadable

class URL(BaseModel):
    user: str = getpass.getuser()
    host: str
    port: int = 22
    path: str = "."
    
class Repo(Loadable('remote'), BaseModel):
    """
    A description of a file respository. May be local or remote.
    """
    name: str
    uuid: UUID
    url: URL | Path
    key_format: FileKeyType

    def filestore(self) -> FileStore:
        """Return a FileStore instance for this repository. The type of FileStore returned depends on the URL scheme"""
        if isinstance(self.url, Path):
            return AnnexFS(
                root=self.url,
                file_key_format=self.key_format
            )
        else:
            return SftpFileStore(
                host=self.url.host,
                port=self.url.port,
                username=self.url.user,
                path=self.url.path,
                file_key_format=self.key_format
            )

    def files_dir(self) -> Path:
        """
        Returns the directory containing the files for this repository.

        This may be a local path, or a path on a remote server.
        """
        if self.files_url.startswith("file://"):
            return Path(self.files_url[7:])
        elif self.files_url.startswith("ssh://"):
            return Path(self.files_url.split(":", 2)[2])
        else:
            raise ValueError(f"Unsupported URL scheme in files_url, must begin with ssh:// or file://: {self.files_url}")
