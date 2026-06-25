#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, List
from typing_extensions import Optional

from dolt_annex.datatypes.common import MySQLConnection
from dolt_annex.datatypes.config.gallerydl_config import GalleryDLConfig
from dolt_annex.datatypes.filestore_config import FilestoreConfig
from dolt_annex.datatypes.pydantic import StrictBaseModel
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.hash_size_extension import MD5HSe, Sha1HSe, Sha256HSe

class UserConfig(StrictBaseModel):
    email: str
    name: str

class DoltConfig(StrictBaseModel):
    default_remote: str = "origin"
    default_commit_message: str = "update dolt-annex"
    connection: MySQLConnection = MySQLConnection()
    spawn_dolt_server: bool = True
    dolt_dir: Path = Path("dolt")

def default_ssh_config_path() -> Optional[Path]:
    path = Path("~/.ssh/config").expanduser()
    if path.exists():
        return path
    return None

def resolve_path(path: Optional[Path]) -> Optional[str]:
    if path is not None:
        return path.expanduser().as_posix()
    return None

class SshSettings(StrictBaseModel):
    ssh_config: Optional[Path] = default_ssh_config_path()
    known_hosts: Optional[Path] = None
    encrypted_ssh_key: bool = False
    client_key: Optional[Path] = None

class Config(StrictBaseModel):
    """Global configuration settings"""
    user: UserConfig = UserConfig(name="user", email="user@localhost")
    dolt: DoltConfig = DoltConfig()
    ssh: SshSettings = SshSettings()
    filestore: FilestoreConfig = FilestoreConfig()
    gallery_dl: GalleryDLConfig = GalleryDLConfig()
    local_repo_name: str = "__local__"
    default_annex_remote: str = "origin"
    default_file_key_type: FileKeyType = Sha256HSe
    default_alternate_key_types: List[FileKeyType] = [Sha1HSe, MD5HSe]

    def get_default_repo(self) -> RepoModel:
        return RepoModel.must_load(self.local_repo_name)

    @asynccontextmanager
    async def open_default_repo(self) -> AsyncGenerator[Repo]:
        async with Repo.open(self, self.local_repo_name) as filestore:
            yield filestore
