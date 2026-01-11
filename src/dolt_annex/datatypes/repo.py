#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncGenerator, Optional
from uuid import UUID
import pathlib

from pydantic import SerializeAsAny

from dolt_annex.filestore.base import FileStore, FileStoreModel
from dolt_annex.file_keys import FileKeyType

from .loader import Loadable

if TYPE_CHECKING:
    from dolt_annex.datatypes.config import Config
    from typing_extensions import Self
    
class RepoModel(Loadable, extension="repo", config_dir=pathlib.Path("repos")):
    """
    A description of a file respository. May be local or remote.
    """
    uuid: UUID
    filestore: SerializeAsAny[FileStoreModel]
    key_format: FileKeyType

@dataclass
class Repo:
    """
    A file respository whose filestore is open. May be local or remote.
    """
    name: str
    uuid: UUID
    filestore: FileStore
    key_format: FileKeyType

    @classmethod
    @asynccontextmanager
    async def open(cls, config: Config, name: Optional[str]) -> AsyncGenerator[Self]:
        if name is None:
            name = config.local_repo_name
        repo_model = RepoModel.must_load(name)
        async with repo_model.filestore.open(config) as filestore:
            yield cls(
                name=name,
                uuid=repo_model.uuid,
                filestore=filestore,
                key_format=repo_model.key_format
            )