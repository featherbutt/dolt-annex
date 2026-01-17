from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import uuid

from plumbum import cli, local

from dolt_annex.application import Application
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.filestore.annexfs import AnnexFS, AnnexFSModel
from dolt_annex.datatypes.config import Config
from dolt_annex.data import data_dir

def is_wsl():
    """Check if running in Windows Subsystem for Linux"""
    return os.path.exists("/proc/sys/fs/binfmt_misc/WSLInterop")

@dataclass
class InitConfig:
    init_dolt: bool # Do we need to initialize the Dolt repository?
    dolt_url: str   # Are we adding a remote Dolt repository?
    remote_name: str
    
class Init(cli.Application):
    """Initialize the Dolt and Git repositories"""

    parent: Application

    no_dolt = cli.Flag(
        "--no-dolt",
        help = "Don't initialize the Dolt repository",
    )

    dolt_url = cli.SwitchAttr(
        "--dolt-url",
        str,
        envname = "DA_DOLT_URL",
    )

    remote_name = cli.SwitchAttr(
        "--name",
        str,
        envname = "DA_REMOTE_NAME",
    )

    def main(self, *args):
        if args:
            print("Unexpected positional arguments: ", args)
            self.help()
            return 1
        
        init_config = InitConfig(
            init_dolt = not self.no_dolt,
            dolt_url = self.dolt_url,
            remote_name = self.remote_name
        )
        base_config: Config = self.parent.config
        # TODO: The init command using the existing config is a bit chicken-and-egg.
        # There's probably a better way to handle this.
        # We should ensure the default dolt port is set so that it can connect to
        # a running dolt sql-server.
        if base_config.dolt.connection.port is None:
            base_config.dolt.connection.port = 3306

        local_repo = RepoModel.load(base_config.local_repo_name)
        if local_repo is None:
            local_repo = RepoModel(
                name=base_config.local_repo_name,
                uuid=uuid.uuid4(),
                filestore=AnnexFSModel(root=Path("./annex")),
                key_format=base_config.default_file_key_type,
            )
            local_repo.save()
        do_init(self.parent.config, init_config)
        return 0

def do_init(base_config: Config, init_config: InitConfig):
    # Things that need to be created:
    # - dolt directory/repository (if it doesn't exist)
    # - If a dolt-url is provided, add it as a remote to the dolt repository
    # - If a dolt-url is provided, fetch from it and create a branch for this UUID?

    if init_config.init_dolt:
        dolt_dir = base_config.dolt.dolt_dir
        if not dolt_dir.exists() or not (dolt_dir / ".dolt").exists():
            print(f"Dolt directory {dolt_dir} does not exist. Creating it.")
            dolt_dir.mkdir(parents=True, exist_ok=True)
            shutil.copytree(data_dir / "dolt_base" / ".dolt", dolt_dir / ".dolt")
            print(f"Initialized Dolt repository in {dolt_dir}")
            # Add dolt remote if it was provided
            dolt = local.cmd.dolt.with_cwd(base_config.dolt.dolt_dir)
            dolt("config", "--local", "--add", "push.autoSetupRemote", "true")
            if init_config.dolt_url:
                dolt("remote", "add", init_config.remote_name, init_config.dolt_url)
                dolt("fetch", init_config.remote_name)

    # TODO: Add .remote file?

    # Create config file
    if not Path("config.json").exists():

        with open("config.json", "w", encoding="utf-8") as f:
            path = Path("config.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write(base_config.model_dump_json(ensure_ascii=False, indent=4))
            print("Created config.json")