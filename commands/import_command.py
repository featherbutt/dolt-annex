import os
import shutil
from typing import Callable, Dict

from plumbum import cli # type: ignore

import importers
from application import Application
from downloader import GitAnnexDownloader, move_files, MoveFunction
from logger import logger
from type_hints import AnnexKey, PathLike

class Import(cli.Application):
    """Import a file or directory into the annex and database"""

    parent: Application
    files: Dict[AnnexKey, PathLike]

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
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
        str,
        help="Prepend this to the relative path of each imported file to get the url",
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

    def get_importer(self, downloader: GitAnnexDownloader) -> importers.Importer:
        if self.from_other_annex:
            return importers.OtherAnnexImporter(self.from_other_annex)
        elif self.url_prefix:
            return importers.DirectoryImporter(self.url_prefix)
        elif self.from_md5:
            return importers.MD5Importer()
        elif self.from_falr:
            return importers.FALRImporter(downloader.dolt_server, "filenames", "filenames")
        else:
            return importers.NullImporter()
        
    def get_move_function(self) -> MoveFunction:
        if self.copy:
            return shutil.copy
        elif self.symlink:
            def move_and_symlink(src: str, dst: str):
                shutil.move(src, dst)
                os.symlink(dst, src)
            return move_and_symlink
        else:
            return shutil.move

    def main(self, *files_or_directories: str):
        self.files = {}

        if not self.copy and not self.move and not self.symlink:
            raise ValueError("Must specify --copy, --move, or --symlink")
        
        move_function = self.get_move_function()
        follow_symlinks = (self.symlinks == "follow")

        with self.parent.Downloader(self.batch_size) as downloader:
            downloader.cache.add_flush_hook(lambda: move_files(downloader, move_function, self.files))
            importer = self.get_importer(downloader)
            for file_or_directory in files_or_directories:
                self.import_path(downloader, file_or_directory, importer, follow_symlinks)

    def import_path(self, downloader: GitAnnexDownloader, file_or_directory: str, importer: importers.Importer, follow_symlinks: bool):
        """Import a file or directory into the annex"""
        if os.path.isfile(file_or_directory):
            self.import_file(downloader, file_or_directory, importer, follow_symlinks)
        elif os.path.isdir(file_or_directory):
            self.import_directory(downloader, file_or_directory, importer, follow_symlinks)
        else:
            raise ValueError(f"Path {file_or_directory} is not a file or directory")

    def import_directory(self, downloader: GitAnnexDownloader, path: str, importer: importers.Importer, follow_symlinks: bool):
        """Import a directory into the annex"""
        logger.debug(f"Importing directory {path}")
        for root, _, files in os.walk(path):
            for file in files:
                self.import_file(downloader, os.path.join(root, file), importer, follow_symlinks)

    def import_file(self, downloader: GitAnnexDownloader, path: str, importer: importers.Importer, follow_symlinks: bool):
        """Import a file into the annex"""
        extension = os.path.splitext(path)[1]
        if len(extension) > downloader.max_extension_length+1:
            return
        # catch both regular symlinks and windows shortcuts
        is_symlink = os.path.islink(path) or extension == 'lnk'
        original_path = os.path.abspath(path)
        if is_symlink:
            if not follow_symlinks:
                return
            else:
                path = os.path.realpath(path)
        if importer and importer.skip(path):
            return
        logger.debug(f"Importing file {path}")
        abs_path = os.path.abspath(path)
        key = downloader.git.annex.calckey(abs_path)
        downloader.add_local_source(key)

        if importer:
            urls = importer.url(original_path, path)
            for url in urls:
                downloader.update_database(url, key)
            if (md5 := importer.md5(original_path)):
                downloader.record_md5(md5, key)

        self.files[AnnexKey(key)] = PathLike(abs_path)