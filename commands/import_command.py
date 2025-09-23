from dataclasses import dataclass
import csv
from io import TextIOWrapper
import os
from pathlib import Path

from typing_extensions import Dict, Iterable

from plumbum import cli # type: ignore

from git import key_from_file
import importers
from application import Application, Downloader
from downloader import move_files
from importers.base import get_importer
from logger import logger
import move_functions
from move_functions import MoveFunction
from remote import Remote
from tables import FileKeyTable
from type_hints import AnnexKey
import context
from annex import AnnexCache

class ImportError(Exception):
    pass

class ImportCsv:
    KEY_ANNEX_KEY = 'annex_key'
    KEY_URL = 'url'
    KEYS = {KEY_ANNEX_KEY, KEY_URL}

    @classmethod
    def import_csv(cls, downloader: AnnexCache, csv_file: TextIOWrapper):
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

            # downloader.update_database(url, annex_key)

@dataclass
class ImportConfig:
    """Configuration for the import command"""
    batch_size: int
    move_function: MoveFunction
    follow_symlinks: bool

class Import(cli.Application):
    """Import a file or directory into the annex and database"""

    parent: Application

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
        excludes = ["--from-md5", "--from-falr", "--move", "--copy", "--symlink"],
    )


    from_md5 = cli.Flag(
        "--from-md5",
        help="Import, assuming the filename is the md5 hash",
        excludes = [],
    )

    from_falr = cli.Flag(
        "--from-falr",
        help="Import, assuming the file path is a FALR id",
        excludes = ["--from-md5"],
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

    importer = cli.SwitchAttr(
        "--importer",
        str,
    )

    table = cli.SwitchAttr(
        "--table",
        str,
        help="The name of the table being written to",
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
        
    def main(self, *files_or_directories: str):

        if not self.from_csv and not self.copy and not self.move and not self.symlink:
            raise ValueError("Must specify --copy, --move, or --symlink")
        
        move_function = self.get_move_function()
        follow_symlinks = (self.symlinks == "follow")

        import_config = ImportConfig(
            batch_size = self.batch_size,
            move_function = move_function,
            follow_symlinks = follow_symlinks,
        )
        table = FileKeyTable.from_name(self.table)
        if not table:
            logger.error(f"Table {self.table} not found")
            return 1

        with Downloader(self.parent.config, import_config.batch_size, table) as downloader:
            if self.from_csv:
                with open(self.from_csv) as csv_file:
                    ImportCsv.import_csv(downloader, csv_file)
            else:
                importer = get_importer(*self.importer.split())
                local_remote = Remote(
                    name="local",
                    uuid=self.parent.config.local_uuid,
                    url=self.parent.config.files_dir.as_posix(),
                )
                do_import(local_remote, import_config, downloader, importer, files_or_directories)

def do_import(remote: Remote, import_config: ImportConfig, downloader: AnnexCache, importer: importers.ImporterBase, files_or_directories: Iterable[str]):
    key_paths: Dict[AnnexKey, Path] = {}
    downloader.add_flush_hook(lambda: move_files(remote, import_config.move_function, key_paths))
    
    def import_path(file_or_directory: Path):
        """Import a file or directory into the annex"""
        if file_or_directory.is_file():
            import_file(file_or_directory)
        elif file_or_directory.is_dir():
            import_directory(file_or_directory)
        else:
            raise ValueError(f"Path {file_or_directory} is not a file or directory")

    def import_directory(path: Path):
        """Import a directory into the annex"""
        logger.debug(f"Importing directory {path}")
        for root, _, files in os.walk(path):
            root_path = Path(root)
            for file in files:
                import_file(root_path / file)

    def import_file(path: Path):
        """Import a file into the annex"""
        extension = path.suffix[1:]
        if len(extension) > downloader.MAX_EXTENSION_LENGTH+1:
            return
        # catch both regular symlinks and windows shortcuts
        is_symlink = path.is_symlink() or extension == '.lnk'
        if is_symlink:
            if not import_config.follow_symlinks:
                return
            else:
                path = path.readlink()
        if importer and importer.skip(path):
            return
        logger.debug(f"Importing file {path}")
        abs_path = Path(path)
        key = key_from_file(abs_path, importer.extension(path))

        if importer:
            key_columns = importer.key_columns(path)
            if key_columns:
                downloader.insert_file_source(key_columns, key, remote.uuid)
            if not key_columns:
                raise ImportError("Importer did not produce a set of key columns, it is not safe to import")

        key_paths[AnnexKey(key)] = abs_path

    for file_or_directory in files_or_directories:
        import_path(Path(file_or_directory))
