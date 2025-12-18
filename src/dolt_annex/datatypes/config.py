#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID
from pydantic import BaseModel
from typing_extensions import Optional, Any

from dolt_annex.datatypes.remote import Repo
from dolt_annex.file_keys import FileKeyType
from dolt_annex.file_keys.sha256e import Sha256e
if TYPE_CHECKING:
    from dolt_annex.filestore import FileStore

class UserConfig(BaseModel):
    email: str = "user@localhost"
    name: str = "user"

class DoltConfig(BaseModel):
    db_name: str = "dolt"
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

class SshSettings(BaseModel):
    ssh_config: Optional[Path] = default_ssh_config_path()
    known_hosts: Optional[Path] = None
    encrypted_ssh_key: bool = False
    client_key: Optional[Path] = None

class Config(BaseModel):
    """Global configuration settings"""
    user: UserConfig = UserConfig(name="user", email="user@localhost")
    dolt: DoltConfig = DoltConfig()
    ssh: SshSettings = SshSettings()
    local_repo_name: str = "__local__"
    default_annex_remote: str = "origin"
    default_file_key_type: FileKeyType = Sha256e

    def get_filestore(self) -> 'FileStore':
        return Repo.must_load(self.local_repo_name).filestore
    
    def get_uuid(self) -> UUID:
        return Repo.must_load(self.local_repo_name).uuid