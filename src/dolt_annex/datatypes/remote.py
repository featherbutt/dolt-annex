#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID
import pathlib

from pydantic import SerializeAsAny

from dolt_annex.filestore.base import FileStore
from dolt_annex.file_keys import FileKeyType

from .loader import Loadable
    
class Repo(Loadable, extension="repo", config_dir=pathlib.Path("remotes")):
    """
    A description of a file respository. May be local or remote.
    """
    uuid: UUID
    filestore: SerializeAsAny[FileStore]
    key_format: FileKeyType
