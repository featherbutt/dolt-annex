from plumbum import cli

from application import Application

class ScanAnnex(cli.Application):
    """Scan the git-annex branch for files to import into Dolt"""

    parent: Application
    
    def main(self):
        with self.parent.Downloader() as downloader:
            downloader.discover_and_populate()
