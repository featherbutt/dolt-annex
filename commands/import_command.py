from collections.abc import Callable
import os
from typing import List
import shutil

from plumbum import cli, local

import importers
import annex
from application import Application
from downloader import GitAnnexDownloader
import importers.base

def import_(downloader: GitAnnexDownloader, file_or_directory: str, importer: importers.Importer, follow_symlinks: bool):
    if os.path.isfile(file_or_directory):
        downloader.import_file(file_or_directory, importer, follow_symlinks)
    elif os.path.isdir(file_or_directory):
        downloader.import_directory(file_or_directory, importer, follow_symlinks)
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

    from_md5 = cli.Flag(
        "--from-md5",
        help="Import, assuming the filename is the md5 hash",
        excludes = ["--from-other-annex", "--url-prefix"],
    )

    from_falr = cli.Flag(
        "--from-falr",
        help="Import, assuming the file path is a FALR id",
        excludes = ["--from-other-annex", "--url-prefix", "--from-md5"],
    )

    move = cli.Flag(
        "--move",
        help="Move imported files into the annex",
        excludes = ["--copy", "--symlink"],
    )

    copy = cli.Flag(
        "--copy",
        help="Copy imported files into the annex",
        excludes = ["--move", "--symlink"],
    )

    symlink = cli.Flag(
        "--symlink",
        help="Copy imported files into the annex",
        excludes = ["--move", "--copy"],
    )

    symlinks = cli.SwitchAttr(
        "--symlinks",
        cli.Set("follow", "skip"),
        help="Whether to follow or skip symlinks",
        default = "skip",
    )

    def get_importer(self, downloader: GitAnnexDownloader) -> importers.base.Importer:
        if self.from_other_annex:
            return importers.OtherAnnexImporter(self.from_other_annex)
        elif self.url_prefix:
            return importers.DirectoryImporter(self.url_prefix)
        elif self.from_md5:
            return importers.MD5Importer()
        elif self.from_falr:
            return importers.FALRImporter(downloader.dolt_server.cursor, "gallery-archive", "fa")
        else:
            return None

    def main(self, *files_or_directories: str):
        if not self.copy and not self.move and not self.symlink:
            raise ValueError("Must specify --copy, --move, or --symlink")
        if self.copy:
            move_function = shutil.copy
        elif self.symlink:
            def move_and_symlink(src: str, dst: str):
                shutil.move(src, dst)
                os.symlink(dst, src)
            move_function = move_and_symlink
        else:
            move_function = shutil.move
        if self.symlinks == "follow":
            follow_symlinks = True
        else:
            follow_symlinks = False
        with self.parent.Downloader(move_function, self.batch_size) as downloader:
            importer = self.get_importer(downloader)
            for file_or_directory in files_or_directories:
                import_(downloader, file_or_directory, importer, follow_symlinks)
