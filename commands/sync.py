from plumbum import cli, local

from application import Application

class Sync(cli.Application):
    """Sync the git-annex branch with the remote"""
    
    parent: Application
    
    def main(self):
        config = self.parent.config
        git = local.cmd.git["-C", config.git_dir]
        git_annex = git["annex"]
        git_annex("sync", "origin", "--no-content")

        dolt = local.cmd.dolt.with_cwd(config.dolt_dir)        
        dolt("add", "--all")
        dolt("commit", "-m", "update")
        dolt("pull", "origin", "main", "--rebase")
        dolt("push", "origin", "main")