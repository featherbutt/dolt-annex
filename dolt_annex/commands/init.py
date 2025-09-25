from dataclasses import dataclass
import os
from pathlib import Path
import uuid

from plumbum import cli, local # type: ignore

from dolt_annex.application import Application, Config

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
        envname = "DA_DOLT_URL",
    )

    remote_name = cli.SwitchAttr(
        "--name",
        envname = "DA_REMOTE_NAME",
    )

    def main(self, *args):
        if args:
            print("Unexpected positional arguments: ", args)
            self.help()
            return 1

        base_config = self.parent.config
        init_config = InitConfig(
            init_dolt = not self.no_dolt,
            dolt_url = self.dolt_url,
            remote_name = self.remote_name,
        )

        do_init(base_config, init_config)

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
    local_uuid = read_uuid()
    if init_config.init_dolt:
        Path(base_config.dolt_dir).mkdir(parents=True, exist_ok=True)

        if init_config.dolt_url:
            #dolt = local.cmd.dolt.with_cwd(base_config.dolt_dir)
            local.cmd.dolt("clone", "--remote", base_config.dolt_remote, init_config.dolt_url, base_config.dolt_dir)
            dolt = local.cmd.dolt.with_cwd(base_config.dolt_dir)
            dolt("fetch")
            #dolt("init", "--name", base_config.name, "--email", base_config.email)
            #dolt("remote", "add", base_config.dolt_remote, init_config.dolt_url)
            #dolt("pull", base_config.dolt_remote, "main")
            #local.cmd.dolt.with_cwd(base_config.dolt_dir)("branch", local_uuid)
        else:
            dolt = local.cmd.dolt.with_cwd(base_config.dolt_dir)
            dolt("init", "--name", base_config.name, "--email", base_config.email)
        dolt = local.cmd.dolt.with_cwd(base_config.dolt_dir)
        dolt("config", "--local", "--add", "push.autoSetupRemote", "true")