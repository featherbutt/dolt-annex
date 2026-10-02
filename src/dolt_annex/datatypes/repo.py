#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, AsyncGenerator, Optional
from warnings import deprecated
from typing_extensions import Annotated
from uuid import UUID
import pathlib

from dolt_annex.datatypes.collection import Collection
from dolt_annex.file_keys.base import Sha256E
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.file_keys import FileKeyType
from dolt_annex.filestore.cas import ContentAddressableStorage

from .loader import Loadable, Named

if TYPE_CHECKING:
    from dolt_annex.datatypes.config import Config
    from typing_extensions import Self
    
class RepoModel(Loadable, extension="repo", config_dir=pathlib.Path("repos")):
    """
    A description of a file respository. May be local or remote.
    """
    uuid: UUID
    filestore: Named[FileStoreModel]
    key_format: Annotated[FileKeyType, deprecated('Repo.key_format is deprecated and will be removed in a future version')] = Sha256E
    alternate_key_formats: list[FileKeyType] = []
    content_addressed: bool = True

    @staticmethod
    def open(config: Config, name: Optional[str]) -> RepoModel:
        if name is None:
            return config.get_default_repo()
        return RepoModel.must_load(name)

@dataclass
class Repo:
    """
    A file respository whose filestore is open. May be local or remote.
    """
    type Id = Collection.Id
    
    name: Optional[str]
    uuid: Id
    filestore: ContentAddressableStorage
    alternate_key_formats: list[FileKeyType] = field(default_factory=list)
    content_addressed: bool = True

    @classmethod
    @asynccontextmanager
    async def open(cls, config: Config, name: Optional[str] = None) -> AsyncGenerator[Self]:
        if name is None:
            model = config.get_default_repo()
        else:
            model = RepoModel.must_load(name)
        async with model.filestore.open(config) as filestore:
            cas = ContentAddressableStorage(config.filestore, filestore, model.alternate_key_formats, model.content_addressed)
            yield cls(
                name=model.name,
                uuid=model.uuid,
                filestore=cas,
                alternate_key_formats=model.alternate_key_formats,
                content_addressed=model.content_addressed,
            )