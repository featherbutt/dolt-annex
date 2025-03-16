from plumbum import cli

from application import Application

class ScanAnnex(cli.Application):
    """Scan the git-annex branch for files to import into Dolt.
    This is useful when migrating a repo to use a new version of the Dolt schema."""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
        default = 1000,
    )

    skip_sources = cli.Flag("--skip_sources")

    skip_urls = cli.Flag("--skip_urls")

    skip_local_keys = cli.Flag("--skip_local_keys")
    
    def main(self):
        with self.parent.Downloader(None, self.batch_size) as downloader:
            if not self.skip_local_keys:
                downloader.mark_present_keys()
            if not self.skip_urls or not self.skip_sources:
                downloader.discover_and_populate(not self.skip_urls, not self.skip_sources)
