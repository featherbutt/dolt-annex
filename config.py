#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextvars import ContextVar
from dataclasses import dataclass

import context
from type_hints import UUID

@dataclass
class Config:
    """Global configuration settings"""
    dolt_dir: str
    dolt_db: str
    dolt_remote: str
    files_dir: str
    email: str
    name: str
    spawn_dolt_server: bool = False
    dolt_server_socket: str = "/tmp/mysql.sock"
    annexcommitmessage: str = "update git-annex"
    auto_push: bool = False

    @property
    def local_uuid(self) -> UUID:
        return context.local_uuid.get()

    def validate(self):
        """Ensure that all required fields are set"""
        for field in ["dolt_dir", "dolt_db", "dolt_remote", "git_dir", "git_remote", "email", "name", "annexcommitmessage"]:
            if getattr(self, field) is None:
                raise ValueError(f"Missing configuration: {field}")

config = ContextVar[Config]('config')

def get_config() -> Config:
    return config.get()

def set_config(new_config: Config):
    config.set(new_config)
