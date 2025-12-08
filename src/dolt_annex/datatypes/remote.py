#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from uuid import UUID

from pydantic import BaseModel

from dolt_annex.file_keys import FileKeyType

from .loader import Loadable
from .common import Connection
    
class Repo(Loadable, extension="repo", config_dir=pathlib.Path("remotes")):
    """
    A description of a file respository. May be local or remote.
    """
    uuid: UUID
    url: Connection | Path
    key_format: FileKeyType
