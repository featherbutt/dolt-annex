from dataclasses import dataclass
import csv
from io import TextIOWrapper
import os

from typing_extensions import Dict, Iterable

from plumbum import cli # type: ignore

import importers
from application import Application, Downloader
from downloader import GitAnnexDownloader, move_files
from logger import logger
import move_functions
from move_functions import MoveFunction
from type_hints import AnnexKey, PathLike

class ImportCsv:
    KEY_ANNEX_KEY = 'annex_key'
    KEY_URL = 'url'
    KEYS = {KEY_ANNEX_KEY, KEY_URL}

    @classmethod
    def import_csv(cls, downloader: GitAnnexDownloader, csv_file: TextIOWrapper):
        """Import annex keys from CSV file"""
        fields_checked = False
        for row in csv.DictReader(csv_file):
            if not fields_checked:
                key_set = set(row.keys())
                if not key_set.issuperset(cls.KEYS):
                    raise ValueError(f'Missing field keys: {cls.KEYS - key_set}')
                fields_checked = True

            annex_key = row[cls.KEY_ANNEX_KEY]
            url = row[cls.KEY_URL]

            downloader.add_local_source(annex_key)
            downloader.update_database(url, annex_key)

@dataclass
class ImportConfig:
    """Configuration for the import command"""
    batch_size: int
    move_function: MoveFunction
    follow_symlinks: bool

class Import(cli.Application):
    """Import a file or directory into the annex and database"""

    parent: Application
    files: Dict[AnnexKey, PathLike]

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
        default = 10000,
    )

    from_csv = cli.SwitchAttr(
        "--from-csv",
        str,
        help="Import annex keys and urls from CSV. File must contain the following fields: f{ImportCsv.KEYS}",
        excludes = ["--from-other-annex", "--url-prefix", "--from-md5", "--from-falr", "--move", "--copy", "--symlink"],
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

    def get_move_function(self) -> MoveFunction:
        """Get the function to move files based on the command line arguments"""
        print(f"Copy: {self.copy}, Move: {self.move}, Symlink: {self.symlink}")
        if self.copy:
            return move_functions.copy
        elif self.symlink:
            return move_functions.move_and_symlink
        else:
            return move_functions.move
        
    def get_importer(self, downloader: GitAnnexDownloader) -> importers.Importer:
        if self.from_other_annex:
            return importers.OtherAnnexImporter(self.from_other_annex)
        elif self.url_prefix:
            return importers.DirectoryImporter(self.url_prefix)
        elif self.from_md5:
            return importers.MD5Importer()
        elif self.from_falr:
            return importers.FALRImporter(downloader.dolt_server, "gallery-archive", "main")
        else:
            return importers.NullImporter()
        
    def main(self, *files_or_directories: str):
        self.files = {}

        if not self.from_csv and not self.copy and not self.move and not self.symlink:
            raise ValueError("Must specify --copy, --move, or --symlink")
        
        move_function = self.get_move_function()
        follow_symlinks = (self.symlinks == "follow")

        import_config = ImportConfig(
            batch_size = self.batch_size,
            move_function = move_function,
            follow_symlinks = follow_symlinks,
        )
        with Downloader(self.parent.config, import_config.batch_size) as downloader:
            if self.from_csv:
                with open(self.from_csv) as csv_file:
                    ImportCsv.import_csv(downloader, csv_file)
            else:
                importer = self.get_importer(downloader)
                do_import(import_config, downloader, importer, files_or_directories)

def do_import(import_config: ImportConfig, downloader: GitAnnexDownloader, importer: importers.Importer, files_or_directories: Iterable[str]):
    key_paths: Dict[AnnexKey, PathLike] = {}
    downloader.cache.add_flush_hook(lambda: move_files(downloader, import_config.move_function, key_paths))
    
    for file_or_directory in files_or_directories:
        import_path(import_config, downloader, file_or_directory, importer, key_paths)

def import_path(config: ImportConfig, downloader: GitAnnexDownloader, file_or_directory: str, importer: importers.Importer, key_paths: Dict[AnnexKey, PathLike]):
    """Import a file or directory into the annex"""
    if os.path.isfile(file_or_directory):
        import_file(config, downloader, file_or_directory, importer, key_paths)
    elif os.path.isdir(file_or_directory):
        import_directory(config, downloader, file_or_directory, importer, key_paths)
    else:
        raise ValueError(f"Path {file_or_directory} is not a file or directory")

def import_directory(config: ImportConfig, downloader: GitAnnexDownloader, path: str, importer: importers.Importer, key_paths: Dict[AnnexKey, PathLike]):
    """Import a directory into the annex"""
    logger.debug(f"Importing directory {path}")
    for root, _, files in os.walk(path):
        for file in files:
            import_file(config, downloader, os.path.join(root, file), importer, key_paths)

def import_file(config: ImportConfig, downloader: GitAnnexDownloader, path: str, importer: importers.Importer, key_paths: Dict[AnnexKey, PathLike]):
    """Import a file into the annex"""
    extension = os.path.splitext(path)[1]
    if len(extension) > downloader.max_extension_length+1:
        return
    # catch both regular symlinks and windows shortcuts
    is_symlink = os.path.islink(path) or extension == '.lnk'
    original_path = os.path.abspath(path)
    if is_symlink:
        if not config.follow_symlinks:
            return
        else:
            path = os.path.realpath(path)
    if importer and importer.skip(path):
        return
    logger.debug(f"Importing file {path}")
    abs_path = PathLike(os.path.abspath(path))
    key = downloader.git.annex.calckey(abs_path)
    # downloader.add_local_source(key)

    if importer:
        urls = importer.url(original_path, path)
        for url in urls:
            downloader.update_database(url, key)
        sid = importer.submission_id(original_path, path)
        if sid:
            downloader.cache.insert_submission_source(sid, downloader.local_uuid)
            downloader.cache.insert_submission_key(sid, key)
        if (md5 := importer.md5(original_path)):
            downloader.record_md5(md5, key)

    key_paths[AnnexKey(key)] = PathLike(abs_path)