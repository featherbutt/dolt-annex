from collections.abc import Callable
import os
from typing import List

from plumbum import cli, local

import importers
import annex
from application import Application
from downloader import GitAnnexDownloader
import importers.base

def import_(downloader: GitAnnexDownloader, file_or_directory: str, importer: importers.Importer):
        if os.path.isfile(file_or_directory):
            downloader.import_file(file_or_directory, importer)
        elif os.path.isdir(file_or_directory):
            downloader.import_directory(file_or_directory, importer)
        else:
            raise ValueError(f"Path {file_or_directory} is not a file or directory")

class Import(cli.Application):
    """Import a file or directory into the annex and database"""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The path of files to process at once",
        default = 1000,
    )

    from_other_annex = cli.SwitchAttr(
        "--from-other-annex",
        cli.ExistingDirectory,
        help="The path of another git-annex repository to import from",
        excludes = ["--url-prefix"],
    )

    url_prefix = cli.SwitchAttr(
        "--url-prefix",
        cli.ExistingDirectory,
        help="The path of a directory to import from",
        excludes = ["--from-other-annex"],
    )

    from_other_git = cli.SwitchAttr(
        "--from-other-git",
        cli.ExistingDirectory,
        help="The path of another git repository to import from",
        excludes = ["--from-other-annex", "--url-prefix"],
    )

    from_md5 = cli.Flag(
        "--from-md5",
        help="Import, assuming the filename is the md5 hash",
        excludes = ["--from-other-annex", "--url-prefix", "--from-other-git", "--from-e621", "--from-gelbooru"],
    )

    def get_importer(self):
        if self.from_other_annex:
            return importers.OtherAnnexImporter(self.from_other_annex)
        elif self.url_prefix:
            return importers.DirectoryImporter(self.url_prefix)
        elif self.from_md5:
            return importers.MD5Importer()
        else:
            return None

    def main(self, *files_or_directories: str):
        importer = self.get_importer()
        with self.parent.Downloader(self.batch_size) as downloader:
            for file_or_directory in files_or_directories:
                if self.from_other_git:
                    downloader.import_git_branch(self.from_other_git, file_or_directory, importer)
                else:
                    import_(downloader, file_or_directory, importer)
