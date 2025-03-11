import os
from plumbum import cli, local

from application import Application

def is_wsl():
    return os.path.exists("/proc/sys/fs/binfmt_misc/WSLInterop")

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

    remote_name = cli.SwitchAttr(
        "--name",
        envname = "DA_REMOTE_NAME",
        mandatory = True,
    )

    def main(self):
        config = self.parent.config

        if not self.no_dolt:
            dolt = local.cmd.dolt.with_cwd(config.dolt_dir)
            dolt("clone", config.dolt_remote, config.dolt_dir)
            #dolt("init")
            #dolt("remote", "add", "origin", config.dolt_remote)
            #dolt("pull", "origin", "main")

        if not self.no_git:
            git = local.cmd.git["-C", config.git_dir]

            git_config = git["config", "--local"]
            git_annex = git["annex"]

            git("init", "--bare")
            
            git_config("user.name", config.name)
            git_config("user.email", config.email)
            git("-c", "annex.tune.objecthashlower=true", "annex", "init", self.remote_name)
            git_config("annex.commitmessage", config.annexcommitmessage)
            git_config("annex.maxextensions", "1")
            if is_wsl():
                git_config("annex.crippledfilesystem", "true")
            #git("remote", "add", "origin", git_remote)
            #git("fetch", "origin", "git-annex")
            
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