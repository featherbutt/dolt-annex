#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
import json
import os
from pathlib import Path

from plumbum import cli

from dolt_annex.datatypes.table import DatasetSchema, DatasetSource

from .config import Config, set_config
from .dolt import DoltSqlServer
from .table import Dataset
class Env:
    DOLT_DIR = "DA_DOLT_DIR"
    SPAWN_DOLT_SERVER = "DA_SPAWN_DOLT_SERVER"
    DOLT_SERVER_SOCKET = "DA_DOLT_SERVER_SOCKET"
    DOLT_DB = "DA_DOLT_DB"
    DOLT_REMOTE = "DA_DOLT_REMOTE"
    EMAIL = "DA_EMAIL"
    NAME = "DA_NAME"
    ANNEX_COMMIT_MESSAGE = "DA_ANNEX_COMMIT_MESSAGE"
    AUTO_PUSH = "DA_AUTO_PUSH"

class Application(cli.Application):
    """The top level CLI command"""
    PROGNAME = "dolt-annex"
    VERSION = "0.1"

    def __init__(self, *args):
        super().__init__(*args)
        self.config = Config(
            dolt_dir = None,
            dolt_db = None,
            files_dir = None,
            dolt_remote = None,
            email = None,
            name = None,
        )

    @cli.switch(['-c', '--config'], cli.ExistingFile)
    def set_config(self, path):
        with open(path) as f:
            try:
                config_json = json.load(f)
                for key, value in config_json.items():
                    if key == "files_dir":
                        value = Path(value)
                        if not value.exists():
                            raise ValueError(f"Files directory {value} does not exist")
                    setattr(self.config, key, value)
            except json.JSONDecodeError as e:
                print(f"Error parsing config file {path}: {e}")
                raise

    dolt_dir = cli.SwitchAttr("--dolt-dir", cli.ExistingDirectory, envname=Env.DOLT_DIR)

    spawn_dolt_server = cli.Flag("--spawn-dolt-server", envname=Env.SPAWN_DOLT_SERVER,
                                 help = "If set, spawn a new Dolt server instead of connecting to an existing one.")

    dolt_server_socket = cli.SwitchAttr("--dolt-server-socket", str, envname=Env.DOLT_SERVER_SOCKET,
                                        help = "The UNIX socket to use for the Dolt server.")

    dolt_db = cli.SwitchAttr("--dolt-db", str, envname=Env.DOLT_DB)

    dolt_remote = cli.SwitchAttr("--dolt-remote", str, envname=Env.DOLT_REMOTE)

    email = cli.SwitchAttr("--email", str, envname=Env.EMAIL)

    name = cli.SwitchAttr("--name", str, envname=Env.NAME)

    annexcommitmessage = cli.SwitchAttr("--annexcommitmessage", str, envname=Env.ANNEX_COMMIT_MESSAGE)

    def main(self, *args):
        # Set each config parameter in order of preference:
        # 1. Command line argument or environment variable
        # 2. Existing config file passed in with -c
        # 3. Default value
        self.config.dolt_dir = self.dolt_dir or self.config.dolt_dir or "./dolt"
        self.config.spawn_dolt_server = self.spawn_dolt_server or self.config.spawn_dolt_server
        self.config.dolt_server_socket = self.dolt_server_socket or self.config.dolt_server_socket
        self.config.dolt_db = self.dolt_db or self.config.dolt_db
        self.config.dolt_remote = self.dolt_remote or self.config.dolt_remote or "origin"
        self.config.email = self.email or self.config.email or "user@localhost"
        self.config.name = self.name or self.config.name or "user"
        self.config.annexcommitmessage = self.annexcommitmessage or self.config.annexcommitmessage or "update git-annex"
       
        if self.nested_command is None:
            self.help()
            return 0
        if args:
            print(f"Unknown command: {args[0]}")
            return 1
        self.config.validate()
        set_config(self.config)

@contextmanager
def Downloader(base_config: Config, db_batch_size, dataset: DatasetSchema):
    db_config = {
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": 3306,
    }
    if os.name == 'nt':
        db_config["host"] = base_config.dolt_host
    else:
        db_config["unix_socket"] = base_config.dolt_server_socket

    dataset_source = DatasetSource(
        schema=dataset,
        repo=base_config.local_repo(),
    )
    with (
        DoltSqlServer(base_config.dolt_dir, base_config.dolt_db, db_config, base_config.spawn_dolt_server) as dolt_server,
        Dataset(dolt_server, dataset_source, base_config.auto_push, db_batch_size) as cache
    ):
        yield cache
    
