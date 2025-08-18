from plumbum import cli, local

from application import Application

class Sync(cli.Application):
    """Sync the git-annex branch with the remote"""
    
    parent: Application
    
    dont_sync_dolt = cli.Flag(
        "--dont-sync-dolt",
        help = "Don't sync the dolt branch",
    )

    def main(self):
        config = self.parent.config

        if not self.dont_sync_dolt:
            dolt = local.cmd.dolt.with_cwd(config.dolt_dir)        
            dolt("add", "--all")
            dolt("commit", "-m", "update")
            dolt("pull", "origin", "main", "--rebase")
            dolt("push", "origin", "main")