#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from .loader import Loadable

@dataclass
class Repo(Loadable("remote")):
    """
    A description of a file respository. May be local or remote.
    """
    name: str
    uuid: UUID
    files_url: str
    dolt_remote: str = ""

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
            return Path(self.files_url)

