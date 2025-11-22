from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import uuid

from plumbum import cli, local

from dolt_annex.application import Application
from dolt_annex.datatypes.config import Config
from dolt_annex.gallery_dl_plugin import skip_db_path
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
        do_init(self.parent.config, init_config)
        return 0

        

def read_uuid() -> uuid.UUID:
    try:
        with open("uuid", encoding="utf-8") as fd:
            local_uuid = uuid.UUID(fd.read().strip())
    except FileNotFoundError:
        # Generate a new UUID if not found
        local_uuid = uuid.uuid4()
        with open("uuid", "w", encoding="utf-8") as fd:
            fd.write(str(local_uuid))
    return local_uuid

def do_init(base_config: Config, init_config: InitConfig):
    # Things that need to be created:
    # - dolt directory/repository (if it doesn't exist)
    # - skip.sqlite3 database (if it doesn't exist)
    # - uuid file (if it doesn't exist)
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

    local_uuid = read_uuid()
    print(f"Local UUID: {local_uuid}")

    if not Path("skip.sqlite3").exists():
        shutil.copy(skip_db_path, "skip.sqlite3")

    # TODO: Add .remote file?

    # Create config file
    if not Path("config.json").exists():

        with open("config.json", "w", encoding="utf-8") as f:
            path = Path("config.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write(base_config.model_dump_json(ensure_ascii=False, indent=4))
            print("Created config.json")