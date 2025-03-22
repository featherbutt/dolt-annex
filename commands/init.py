from dataclasses import dataclass
import os
from pathlib import Path
from plumbum import cli, local # type: ignore

from application import Application, Config
from db import PERSONAL_BRANCH_INIT_SQL, SHARED_BRANCH_INIT_SQL

def is_wsl():
    """Check if running in Windows Subsystem for Linux"""
    return os.path.exists("/proc/sys/fs/binfmt_misc/WSLInterop")

@dataclass
class InitConfig:
    init_git: bool
    init_dolt: bool
    git_url: str
    dolt_url: str
    remote_name: str
    
class Init(cli.Application):
    """Initialize the Dolt and Git repositories"""

    parent: Application

    no_dolt = cli.Flag(
        "--no-dolt",
        help = "Don't initialize the Dolt repository",
    )

    no_git = cli.Flag(
        "--no-git",
        help = "Don't initialize the Git repository",
    )

    git_url = cli.SwitchAttr(
        "--git-url",
        envname = "DA_GIT_URL",
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
            init_git = not self.no_git,
            init_dolt = not self.no_dolt,
            git_url = self.git_url,
            dolt_url = self.dolt_url,
            remote_name = self.remote_name,
        )

        do_init(base_config, init_config)

def do_init(base_config: Config, init_config: InitConfig):
    git = local.cmd.git["-C", base_config.git_dir]

    git_config = git["config", "--local"]
    git_annex = git["annex"]

    if init_config.init_git:
        Path(base_config.git_dir).mkdir(parents=True, exist_ok=True)
        git("init", "--bare")
        
        git_config("user.name", base_config.name)
        git_config("user.email", base_config.email)
        git("-c", "annex.tune.objecthashlower=true", "annex", "init", init_config.remote_name)
        git_config("annex.commitmessage", base_config.annexcommitmessage)
        git_config("annex.maxextensions", "1")
        if is_wsl():
            git_config("annex.crippledfilesystem", "true")
        
        if init_config.git_url:
            git("remote", "add", base_config.git_remote, init_config.git_url)
            git("fetch", base_config.git_remote, "git-annex")
        
        git_annex("mincopies", "3")
        git_annex("numcopies", "3")

        git_annex("initremote", "--sameas=web", "tor", "type=web", "urlinclude=*//*.onion/*")
        git_config("remote.tor.cost", "300")

        git_annex("initremote", "--sameas=web", "nontor", "type=web", "urlexclude=*//*.onion/*")
        git_config("remote.nontor.cost", "100")

        git_annex("enableremote", "tor")
        git_annex("enableremote", "nontor")

        git_annex("untrust", "web")
        git_annex("untrust", "tor")
        git_annex("untrust", "nontor")

    if init_config.init_dolt:
        Path(base_config.dolt_dir).mkdir(parents=True, exist_ok=True)

        local_uuid = git_config("annex.uuid").strip()

        if init_config.dolt_url:
            #dolt = local.cmd.dolt.with_cwd(base_config.dolt_dir)
            local.cmd.dolt("clone", "--remote", base_config.dolt_remote, init_config.dolt_url, base_config.dolt_dir)
            #dolt("init", "--name", base_config.name, "--email", base_config.email)
            #dolt("remote", "add", base_config.dolt_remote, init_config.dolt_url)
            #dolt("pull", base_config.dolt_remote, "main")
            #local.cmd.dolt.with_cwd(base_config.dolt_dir)("branch", local_uuid)
        else:
            dolt = local.cmd.dolt.with_cwd(base_config.dolt_dir)
            dolt("init", "--name", base_config.name, "--email", base_config.email)
            dolt("checkout", '-b', local_uuid)
            dolt("sql", "-q", PERSONAL_BRANCH_INIT_SQL)
            dolt("checkout", "main")
            dolt("sql", "-q", SHARED_BRANCH_INIT_SQL)
            