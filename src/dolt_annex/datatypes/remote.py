#!/usr/bin/env python
# -*- coding: utf-8 -*-

import getpass
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel

from dolt_annex.file_keys import FileKeyType
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.base import ContentAddressableStorage
from dolt_annex.filestore.sftp import SftpFileStore

from .loader import Loadable
from .common import Connection
    
class Repo(Loadable('remote'), BaseModel):
    """
    A description of a file respository. May be local or remote.
    """
    name: str
    uuid: UUID
    url: Connection | Path
    key_format: FileKeyType

    def filestore(self) -> ContentAddressableStorage:
        """Return a FileStore instance for this repository. The type of FileStore returned depends on the URL scheme"""
        if isinstance(self.url, Path):
            return ContentAddressableStorage(
                file_store=AnnexFS(
                    root=self.url,
                ),
                file_key_format=self.key_format
            )
        else:
            return ContentAddressableStorage(
                file_store=SftpFileStore(url=self.url),
                file_key_format=self.key_format
            )

    def files_dir(self) -> Path:
        """
        Returns the directory containing the files for this repository.

        This may be a local path, or a path on a remote server.
        """
        if isinstance(self.url, Path):
            return self.url
        else:
            return Path(self.url.path)