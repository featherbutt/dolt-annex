#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from uuid import UUID

from pydantic import BaseModel

from dolt_annex.file_keys import FileKeyType

from .loader import Loadable
from .common import Connection
    
class Repo(Loadable('remote'), BaseModel):
    """
    A description of a file respository. May be local or remote.
    """
    uuid: UUID
    url: Connection | Path
    key_format: FileKeyType

    def files_dir(self) -> Path:
        """
        Returns the directory containing the files for this repository.

        This may be a local path, or a path on a remote server.
        """
        if isinstance(self.url, Path):
            return self.url
        else:
            return Path(self.url.path)