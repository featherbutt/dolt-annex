#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional
from warnings import deprecated
from typing_extensions import Annotated
from uuid import UUID
import pathlib

from dolt_annex.datatypes.collection import Collection
from dolt_annex.file_keys.base import Sha256E
from dolt_annex.filestore.base import FileStore, FileStoreModel
from dolt_annex.file_keys import FileKeyType
from dolt_annex.filestore.cas import ContentAddressableStorage

from .loader import Loadable, Named

if TYPE_CHECKING:
    from dolt_annex.config.base import Config
    from typing_extensions import Self
    
class RepoModel(Loadable, extension="repo", config_dir=pathlib.Path("repos")):
    """
    A description of a file respository. Deprecated, because their entire state can be represented by the filestore itself.
    """
    uuid: UUID
    filestore: Named[FileStoreModel]
    key_format: Annotated[FileKeyType, deprecated('Repo.key_format is deprecated and will be removed in a future version')] = Sha256E
    alternate_key_formats: Annotated[list[FileKeyType], deprecated('Repo.alternate_key_formats is deprecated and will be removed in a future version')] = []
    content_addressed: bool = True

    @staticmethod
    def open(config: Config, name: Optional[str]) -> RepoModel:
        # Three sources, all must match
        def repo_model_from_name(name: str) -> RepoModel:
            repo_model = RepoModel.load(name)
            filestore_model = FileStoreModel.load(name)
            if repo_model is None:
                if filestore_model is None:
                    raise ValueError(f"Neither repo nor filestore could be loaded for name {name}")
                else:
                    if filestore_model.uuid is None:
                        raise ValueError(f"Filestore UUID is None for name {name}")
                    repo_model = RepoModel(
                        name=filestore_model.name,
                        uuid=filestore_model.uuid,
                        filestore=filestore_model,
                    )
            else:
                if filestore_model is not None:
                    if filestore_model.uuid is None:
                        filestore_model.uuid = repo_model.uuid
                    elif filestore_model.uuid != repo_model.uuid:
                        raise ValueError(f"Filestore UUID {filestore_model.uuid} does not match repo UUID {repo_model.uuid}")
                    if repo_model.filestore != filestore_model:
                        raise ValueError(f"Filestore model {filestore_model} does not match repo's filestore {repo_model.filestore}")
            return repo_model

        def get_repo_model(name: Optional[str]) -> RepoModel:
            if name is not None:
                return repo_model_from_name(name)
            
            repo_model_or_name = config.local_repo
            if isinstance(repo_model_or_name, str):
                return repo_model_from_name(repo_model_or_name)
            else:
                return repo_model_or_name

        repo_model = get_repo_model(name)
                
        if repo_model.filestore.name is None:
            repo_model.filestore.name = repo_model.name
        elif repo_model.filestore.name != repo_model.name:
            raise ValueError(f"Filestore name {repo_model.filestore.name} does not match repo name {repo_model.name}")

        if repo_model.filestore.uuid is None:
            repo_model.filestore.uuid = repo_model.uuid
        elif repo_model.filestore.uuid != repo_model.uuid:
            raise ValueError(f"Filestore UUID {repo_model.filestore.uuid} does not match repo UUID {repo_model.uuid}")

        return repo_model

@dataclass
class Repo:
    """
    A file respository whose filestore is open. May be local or remote.
    """
    type Id = Collection.Id
    
    name: Optional[str]
    uuid: Id
    filestore: ContentAddressableStorage
    content_addressed: bool = True

    @classmethod
    @asynccontextmanager
    async def open(cls, config: Config, name: Optional[str] | RepoModel = None) -> AsyncGenerator[Self]:
        match name:
            case RepoModel():
                model = name
            case _:
                model = RepoModel.open(config, name)
        async with model.filestore.open(config) as filestore:
            if filestore.uuid is None:
                filestore.uuid = model.uuid
            cas = ContentAddressableStorage(config.filestore, filestore, config.default_alternate_key_types, model.content_addressed)
            yield cls(
                name=model.name,
                uuid=model.uuid,
                filestore=cas,
                content_addressed=model.content_addressed,
            )