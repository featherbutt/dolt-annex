from dataclasses import dataclass
import os
from pathlib import Path

from typing_extensions import Dict, Iterable

from plumbum import cli # type: ignore

from dolt_annex import importers, move_functions
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys import key_from_file
from dolt_annex.filestore import get_key_path
from dolt_annex.application import Application
from dolt_annex.importers.base import get_importer
from dolt_annex.logger import logger
from dolt_annex.move_functions import MoveFunction
from dolt_annex.datatypes import AnnexKey, Repo
from dolt_annex.table import Dataset, FileTable

class ImportError(Exception):
    pass

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

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being imported to",
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

        if not self.copy and not self.move and not self.symlink:
            raise ValueError("Must specify --copy, --move, or --symlink")
        
        move_function = self.get_move_function()
        follow_symlinks = (self.symlinks == "follow")

        import_config = ImportConfig(
            batch_size = self.batch_size,
            move_function = move_function,
            follow_symlinks = follow_symlinks,
        )
        dataset_schema = DatasetSchema.must_load(self.dataset)

        with Dataset.connect(self.parent.config, import_config.batch_size, dataset_schema) as dataset:
            importer = get_importer(*self.importer.split())
            do_import(self.parent.config.local_repo(), import_config, dataset, importer, files_or_directories)

def do_import(remote: Repo, import_config: ImportConfig, dataset: Dataset, importer: importers.ImporterBase, files_or_directories: Iterable[str]):
    key_paths: Dict[str, Dict[Path, AnnexKey]] = {}
    for table_name, table in dataset.tables.items():
        key_paths[table_name] = {}
        table.add_flush_hook(move_files, remote, import_config.move_function, key_paths[table_name])
    
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
        if len(extension) > dataset.MAX_EXTENSION_LENGTH+1:
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
                table_name = importer.table_name(path)
                table = dataset.get_table(table_name)
                table.insert_file_source(key_columns, key, remote.uuid)
                key_paths[table_name][abs_path] = key
            if not key_columns:
                raise ImportError("Importer did not produce a set of key columns, it is not safe to import")

    for file_or_directory in files_or_directories:
        import_path(Path(file_or_directory))

def move_files(remote: Repo, move: MoveFunction, files: Dict[Path, AnnexKey]):
    """Move files to the annex"""
    logger.debug("moving annex files")
    for file_path, key in files.items():
        key_path = remote.files_dir() / get_key_path(key)
        move(file_path, key_path)
    files.clear()
