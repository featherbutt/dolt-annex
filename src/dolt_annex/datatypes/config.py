#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from uuid import UUID
from pydantic import BaseModel
from typing_extensions import Optional, Any

from dolt_annex.filestore import FileStore

class UserConfig(BaseModel):
    email: str = "user@localhost"
    name: str = "user"

class DoltConfig(BaseModel):
    db_name: str = "dolt"
    default_remote: str = "origin"
    default_commit_message: str = "update dolt-annex"
    connection: dict[str, Any] = {}
    spawn_dolt_server: bool = True
    dolt_dir: Path = Path(".dolt")
    port: Optional[int] = None
    hostname: str = "localhost"
    user: str = "root"
    server_socket: Optional[Path] = None
    autocommit: bool = False

class SshSettings(BaseModel):
    ssh_config: Optional[Path] = None
    known_hosts: Optional[Path] = None
    encrypted_ssh_key: bool = False

class Config(BaseModel):
    """Global configuration settings"""
    user: UserConfig = UserConfig()
    dolt: DoltConfig = DoltConfig()
    ssh: SshSettings = SshSettings()
    uuid: Optional[UUID] = None
    filestore: Optional[FileStore] = None
    repo_directory: Path = Path('.')
    default_annex_remote: str = "origin"

    def get_filestore(self) -> FileStore:
        if self.filestore is None:
            raise ValueError("No filestore configured in config")
        return self.filestore
    
    def get_uuid(self) -> UUID:
        if self.uuid is None:
            raise ValueError("No UUID configured in config")
        return self.uuid