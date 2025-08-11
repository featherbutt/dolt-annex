from contextlib import contextmanager
import json

from plumbum import cli # type: ignore

import context
from config import Config
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
import dry_run
from annex import AnnexCache
from type_hints import UUID
class Env:
    DOLT_DIR = "DA_DOLT_DIR"
    SPAWN_DOLT_SERVER = "DA_SPAWN_DOLT_SERVER"
    DOLT_SERVER_SOCKET = "DA_DOLT_SERVER_SOCKET"
    DOLT_DB = "DA_DOLT_DB"
    DOLT_REMOTE = "DA_DOLT_REMOTE"
    GIT_DIR = "DA_GIT_DIR"
    GIT_REMOTE = "DA_GIT_REMOTE"
    EMAIL = "DA_EMAIL"
    NAME = "DA_NAME"
    ANNEX_COMMIT_MESSAGE = "DA_ANNEX_COMMIT_MESSAGE"
    AUTO_PUSH = "DA_AUTO_PUSH"
    NO_GC = "DA_NO_GC"

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

    @cli.switch("--dry-run", help="Report what would be imported without actually importing")
    def set_dry_run(self):
        dry_run.is_dry_run = True

    @cli.switch(['-c', '--config'], cli.ExistingFile)
    def set_config(self, path):
        with open(path) as f:
            config_json = json.load(f)
            for key, value in config_json.items():
                setattr(self.config, key, value)

    dolt_dir = cli.SwitchAttr("--dolt-dir", cli.ExistingDirectory, envname=Env.DOLT_DIR)

    spawn_dolt_server = cli.Flag("--spawn-dolt-server", envname=Env.SPAWN_DOLT_SERVER,
                                 help = "If set, spawn a new Dolt server instead of connecting to an existing one.")

    dolt_server_socket = cli.SwitchAttr("--dolt-server-socket", str, envname=Env.DOLT_SERVER_SOCKET,
                                        help = "The UNIX socket to use for the Dolt server.")

    dolt_db = cli.SwitchAttr("--dolt-db", str, envname=Env.DOLT_DB)

    dolt_remote = cli.SwitchAttr("--dolt-remote", str, envname=Env.DOLT_REMOTE)

    git_dir = cli.SwitchAttr("--git-dir", cli.ExistingDirectory, envname=Env.GIT_DIR)

    git_remote = cli.SwitchAttr("--git-remote", str, envname=Env.GIT_REMOTE)

    email = cli.SwitchAttr("--email", str, envname=Env.EMAIL)

    name = cli.SwitchAttr("--name", str, envname=Env.NAME)

    annexcommitmessage = cli.SwitchAttr("--annexcommitmessage", str, envname=Env.ANNEX_COMMIT_MESSAGE)

    auto_push = cli.Flag("--auto-push", envname=Env.AUTO_PUSH, help = "If set, automatically push annexed files to origin.")

    no_gc = cli.Flag("--no-gc", envname=Env.NO_GC, help = "If set, automatically push annexed files to origin.")

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
        self.config.git_dir = self.git_dir or self.config.git_dir or "./git"
        self.config.git_remote = self.git_remote or self.config.git_remote or "origin"
        self.config.email = self.email or self.config.email or "user@localhost"
        self.config.name = self.name or self.config.name or "user"
        self.config.annexcommitmessage = self.annexcommitmessage or self.config.annexcommitmessage or "update git-annex"
        if self.no_gc is not None:
            self.config.gc = (not self.no_gc)

        if self.auto_push is not None:
            self.config.auto_push = self.auto_push
        elif self.config.auto_push is None:
            self.config.auto_push = False

        if self.nested_command is None:
            self.help()
            return 0
        if args:
            print(f"Unknown command: {args[0]}")
            return 1
        self.config.validate()

@contextmanager
def Downloader(base_config: Config, db_batch_size):
    db_config = {
        "unix_socket": base_config.dolt_server_socket,
        "user": "root",
        "database": base_config.dolt_db,
        "autocommit": True,
        "port": 3306,
    }
    with (
        DoltSqlServer(base_config.dolt_dir, db_config, base_config.spawn_dolt_server, base_config.gc) as dolt_server,
        AnnexCache(None, dolt_server, base_config.auto_push, db_batch_size) as cache
    ):
        downloader = GitAnnexDownloader(
                cache = cache,
                dolt_server = dolt_server,
        )
        yield downloader
