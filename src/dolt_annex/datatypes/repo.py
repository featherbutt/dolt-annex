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

    @classmethod
    def open(cls, config: Config, name: Optional[str]) -> Self:
        if name is None:
            name = config.local_repo_name
        return cls.must_load(name)

@dataclass
class Repo:
    """
    A file respository whose filestore is open. May be local or remote.
    """
    type Id = Collection.Id
    
    name: str
    uuid: Id
    filestore: ContentAddressableStorage
    key_format: FileKeyType
    alternate_key_formats: list[FileKeyType] = field(default_factory=list)
    content_addressed: bool = True

    @classmethod
    @asynccontextmanager
    async def open(cls, config: Config, name: Optional[str]) -> AsyncGenerator[Self]:
        if name is None:
            name = config.local_repo_name
        repo_model = RepoModel.must_load(name)
        async with repo_model.filestore.open(config) as filestore:
            cas = ContentAddressableStorage(config.filestore, filestore, repo_model.alternate_key_formats, repo_model.content_addressed)
            yield cls(
                name=name,
                uuid=repo_model.uuid,
                filestore=cas,
                alternate_key_formats=repo_model.alternate_key_formats,
                content_addressed=repo_model.content_addressed,
            )