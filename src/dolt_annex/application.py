#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from typing_extensions import Literal

from plumbum import cli
import pyjson5

from dolt_annex.datatypes.config import Config

class Env:
    CONFIG_FILE = "DA_CONFIG"
    FILES_DIR = "DA_FILES_DIR"
    SPAWN_DOLT_SERVER = "DA_SPAWN_DOLT_SERVER"
    DOLT_SERVER_SOCKET = "DA_DOLT_SERVER_SOCKET"
    DOLT_DB = "DA_DOLT_DB"
    DOLT_REMOTE = "DA_DOLT_REMOTE"
    EMAIL = "DA_EMAIL"
    NAME = "DA_NAME"
    ANNEX_COMMIT_MESSAGE = "DA_ANNEX_COMMIT_MESSAGE"
    AUTO_PUSH = "DA_AUTO_PUSH"

default_config_file_locations = [
    Path("config.json5"),
    Path("config.json"),
]

class Application(cli.Application):
    """The top level CLI command"""
    PROGNAME = "dolt-annex"
    VERSION = "0.3.3"

    config_file = cli.SwitchAttr(['-c', '--config'], cli.ExistingFile, envname=Env.CONFIG_FILE)

    files_dir = cli.SwitchAttr("--files-dir", cli.ExistingDirectory, envname=Env.FILES_DIR)

    spawn_dolt_server = cli.Flag("--spawn-dolt-server", envname=Env.SPAWN_DOLT_SERVER,
                                 help = "If set, spawn a new Dolt server instead of connecting to an existing one.")

    dolt_server_socket = cli.SwitchAttr("--dolt-server-socket", str, envname=Env.DOLT_SERVER_SOCKET,
                                        help = "The UNIX socket to use for the Dolt server.")

    dolt_db = cli.SwitchAttr("--dolt-db", str, envname=Env.DOLT_DB)

    email = cli.SwitchAttr("--email", str, envname=Env.EMAIL)

    name = cli.SwitchAttr("--name", str, envname=Env.NAME)

    annexcommitmessage = cli.SwitchAttr("--annexcommitmessage", str, envname=Env.ANNEX_COMMIT_MESSAGE)

    config: Config

    def main(self, *args) -> Literal[0, 1]:
        # Set each config parameter in order of preference:
        # 1. Command line argument
        # 2. environment variable
        # 3. Existing config file passed in with -c
        # 4. Existing config file in default location
        # 5. Default value
        if self.config_file is not None:
            config_file_locations = [Path(self.config_file)]
        else:
            config_file_locations = default_config_file_locations
        for config_path in config_file_locations:
            if config_path.exists():
                with open(config_path, encoding="utf-8") as fd:
                    config_json = pyjson5.load(fd)
                self.config = Config(**config_json)
                break
        else:
            self.config = Config()

        self.config.user.name = self.name or self.config.user.name
        self.config.user.email = self.email or self.config.user.email
        self.config.dolt.default_commit_message = self.annexcommitmessage or self.config.dolt.default_commit_message
        self.config.dolt.spawn_dolt_server = self.spawn_dolt_server or self.config.dolt.spawn_dolt_server
        self.config.dolt.connection.database = self.dolt_db or self.config.dolt.connection.database

        if self.dolt_server_socket:
            self.config.dolt.connection.server_socket = self.dolt_server_socket
        
        if args:
            print(f"Unknown command: {args[0]}")
            return 1
        if self.nested_command is None:
            self.help()
            return 0
        return 0
    
