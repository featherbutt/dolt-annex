from contextlib import contextmanager
from dataclasses import dataclass
import json
from annex import AnnexCache, GitAnnexSettings, MoveFunction
from bup_ext.bup_ext import CommitMetadata
from plumbum import cli

from bup.repo import LocalRepo

from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import dry_run

@dataclass
class Config:
    dolt_dir: str
    dolt_db: str
    dolt_remote: str
    git_dir: str
    git_remote: str
    email: str
    name: str
    spawn_dolt_server: bool = False
    dolt_server_socket: str = "/tmp/mysql.sock"
    annexcommitmessage: str = "update git-annex"
    auto_push: bool = False

    def validate(self):
        for field in ["dolt_dir", "dolt_db", "dolt_remote", "git_dir", "git_remote", "email", "name", "annexcommitmessage"]:
            if getattr(self, field) is None:
                raise ValueError(f"Missing configuration: {field}")

class Application(cli.Application):
    PROGNAME = "dolt-annex"
    VERSION = "0.1"

    def __init__(self, *args):
        super().__init__(*args)
        self.config = Config(
            dolt_dir = None,
            dolt_db = None,
            dolt_remote = None,
            git_dir = None,
            git_remote = None,
            email = None,
            name = None,
        )

    @cli.switch("--dry-run", help="Report what would be imported without actually importing")
    def set_dry_run(self):
        dry_run.is_dry_run = True

    @cli.switch(['-c', '--config'], cli.ExistingFile)
    def set_config(self, path):
        with open(path) as f:
            configJson = json.load(f)
            for key, value in configJson.items():
                setattr(self.config, key, value)

    dolt_dir = cli.SwitchAttr("--dolt-dir", cli.ExistingDirectory, envname="DA_DOLT_DIR")
   
    spawn_dolt_server = cli.Flag("--spawn-dolt-server", envname="DA_SPAWN_DOLT_SERVER",
                                 help = "If set, spawn a new Dolt server instead of connecting to an existing one.")

    dolt_server_socket = cli.SwitchAttr("--dolt-server-socket", str, envname="DA_DOLT_SERVER_SOCKET",
                                        help = "The UNIX socket to use for the Dolt server.")
    
    dolt_db = cli.SwitchAttr("--dolt-db", str, envname="DA_DOLT_DB")

    dolt_remote = cli.SwitchAttr("--dolt-remote", str, envname="DA_DOLT_REMOTE")

    git_dir = cli.SwitchAttr("--git-dir", cli.ExistingDirectory, envname="DA_GIT_DIR")

    git_remote = cli.SwitchAttr("--git-remote", str, envname="DA_GIT_REMOTE")

    email = cli.SwitchAttr("--email", str, envname="DA_EMAIL")

    name = cli.SwitchAttr("--name", str, envname="DA_NAME")

    annexcommitmessage = cli.SwitchAttr("--annexcommitmessage", str, envname="DA_ANNEX_COMMIT_MESSAGE")

    auto_push = cli.Flag("--auto-push", envname="DA_AUTO_PUSH", help = "If set, automatically push annexed files to origin.")

    def main(self, *args):
        if self.dolt_dir is not None:
            self.config.dolt_dir = self.dolt_dir
        if self.spawn_dolt_server is not None:
            self.config.spawn_dolt_server = self.spawn_dolt_server
        if self.dolt_server_socket is not None:
            self.config.dolt_server_socket = self.dolt_server_socket
        if self.dolt_db is not None:
            self.config.dolt_db = self.dolt_db
        if self.dolt_remote is not None:
            self.config.dolt_remote = self.dolt_remote
        if self.git_dir is not None:
            self.config.git_dir = self.git_dir
        if self.git_remote is not None:
            self.config.git_remote = self.git_remote
        if self.email is not None:
            self.config.email = self.email
        if self.name is not None:
            self.config.name = self.name
        if self.annexcommitmessage is not None:
            self.config.annexcommitmessage = self.annexcommitmessage
        if self.auto_push:
            self.config.auto_push = True

        if self.nested_command is None:
            self.help()
            return 0
        if args:
            print(f"Unknown command: {args[0]}")
            return 1
        self.config.validate()

    @contextmanager
    def Downloader(self, move: MoveFunction, db_batch_size):
        db_config = {
            "unix_socket": self.config.dolt_server_socket,
            "user": "root",
            "database": self.config.dolt_db,
            "autocommit": True,
        }
        git = Git(self.config.git_dir)
        commit_metadata = CommitMetadata()
        git_annex_settings = GitAnnexSettings(commit_metadata, 'git-annex')
        with (
            LocalRepo(bytes(self.config.git_dir, encoding='utf8')) as repo,
            DoltSqlServer(self.config.dolt_dir, db_config, self.config.spawn_dolt_server) as dolt_server,
            AnnexCache(repo, dolt_server, git, git_annex_settings, move, db_batch_size) as cache
        ):
            downloader = GitAnnexDownloader(
                    cache = cache,
                    git = git,
                    dolt_server = dolt_server,
                    auto_push = self.config.auto_push,
                    batch_size = db_batch_size,
            )
            yield downloader
            downloader.flush()

