#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID
from typing_extensions import Optional

@dataclass
class Config:
    """Global configuration settings"""
    dolt_dir: Path
    dolt_db: str
    dolt_remote: str
    files_dir: Path
    email: str
    name: str
    spawn_dolt_server: bool = False
    dolt_host: str = "localhost"
    dolt_server_socket: str = "/tmp/mysql.sock"
    annexcommitmessage: str = "update git-annex"
    auto_push: bool = False
    uuid: Optional[UUID] = None
    encrypted_ssh_key: bool = False

    @property
    def local_uuid(self) -> UUID:
        if self.uuid is None:
            with open("uuid", encoding="utf-8") as fd:
                self.uuid = UUID(fd.read().strip())
        return self.uuid

    def validate(self):
        """Ensure that all required fields are set"""
        for field in ["dolt_dir", "dolt_db", "dolt_remote", "email", "name", "annexcommitmessage", "files_dir"]:
            if getattr(self, field) is None:
                raise ValueError(f"Missing configuration: {field}")

config = ContextVar[Config]('config')

def get_config() -> Config:
    return config.get()

def set_config(new_config: Config):
    return config.set(new_config)

@contextmanager
def config_context(new_config: Config):
    token = config.set(new_config)
    try:
        yield
    finally:
        config.reset(token)