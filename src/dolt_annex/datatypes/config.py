#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from uuid import UUID
from pydantic import BaseModel
from typing_extensions import Optional, Any

from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.base import ContentAddressableStorage
from .remote import Repo

class UserConfig(BaseModel):
    email: str = "user@localhost"
    name: str = "user"

class DoltConfig(BaseModel):
    db_name: str = "dolt"
    default_remote: str = "origin"
    default_commit_message: str = "update dolt-annex"
    connection: dict[str, Any] = {}
    spawn_dolt_server: bool = True
    dolt_dir: Path = Path("dolt")
    port: Optional[int] = None
    hostname: str = "localhost"
    user: str = "root"
    server_socket: Optional[Path] = None
    autocommit: bool = False

def default_ssh_config_path() -> Optional[Path]:
    path = Path("~/.ssh/config").expanduser()
    if path.exists():
        return path
    return None

def resolve_path(path: Optional[Path]) -> Optional[str]:
    if path is not None:
        return path.expanduser().as_posix()
    return None

class SshSettings(BaseModel):
    ssh_config: Optional[Path] = default_ssh_config_path()
    known_hosts: Optional[Path] = None
    encrypted_ssh_key: bool = False
    client_key: Optional[Path] = None

class Config(BaseModel):
    """Global configuration settings"""
    user: UserConfig = UserConfig()
    dolt: DoltConfig = DoltConfig()
    ssh: SshSettings = SshSettings()
    uuid: Optional[UUID] = None
    filestore: Optional[FileStore] = None
    repo_directory: Path = Path('.')
    default_annex_remote: str = "origin"
    default_file_key_type: FileKeyType = Sha256e

    def get_filestore(self) -> ContentAddressableStorage:
        if self.filestore is None:
            raise ValueError("No filestore configured in config")
        return ContentAddressableStorage(
            file_store=self.filestore,
            file_key_format=self.default_file_key_type
        )
    
    def get_uuid(self) -> UUID:
        if self.uuid is None:
            raise ValueError("No UUID configured in config")
        return self.uuid