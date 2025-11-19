import asyncio
from dataclasses import dataclass
import os
from pathlib import Path
from uuid import UUID

from typing_extensions import Dict, Iterable

from plumbum import cli # type: ignore

from dolt_annex import importers
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.application import Application
from dolt_annex.file_keys import FileKeyType, get_file_key_type
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.base import maybe_await
from dolt_annex.importers.base import get_importer
from dolt_annex.logger import logger
from dolt_annex.datatypes import AnnexKey
from dolt_annex.table import Dataset

class AnnexImportError(Exception):
    pass

@dataclass
class ImportConfig:
    """Configuration for the import command"""
    batch_size: int
    follow_symlinks: bool
    file_key_type: FileKeyType
    move: bool
    copy: bool
    symlink: bool

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
        help="Ensures that imported files are deleted from their original location after being moved into the annex",
        excludes = ["--copy", "--symlink"],
    )

    copy = cli.Flag(
        "--copy",
        help="Ensures that imported files are copied into the annex and original files are retained",
        excludes = ["--move", "--symlink"],
    )

    symlink = cli.Flag(
        "--symlink",
        help="Moves imported files into the annex and creates symlinks at the original file locations",
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

    file_key_type = cli.SwitchAttr(
        "--file-key-type",
        str,
        help="The type of file key to use",
        default = "Sha256e",
    )
        
    def main(self, *files_or_directories: str):
        asyncio.run(self._main_async(files_or_directories))

    async def _main_async(self, files_or_directories: Iterable[str]):
        base_config = self.parent.config

        if not self.copy and not self.move and not self.symlink:
            raise ValueError("Must specify --copy, --move, or --symlink")
        
        follow_symlinks = (self.symlinks == "follow")

        import_config = ImportConfig(
            batch_size = self.batch_size,
            follow_symlinks = follow_symlinks,
            file_key_type = get_file_key_type(self.file_key_type),
            move=self.move,
            copy=self.copy,
            symlink=self.symlink,
        )
        dataset_schema = DatasetSchema.must_load(self.dataset)

        async with Dataset.connect(base_config, import_config.batch_size, dataset_schema) as dataset:
            importer = get_importer(*self.importer.split())
            await do_import(base_config.get_filestore().file_store, base_config.get_uuid(), import_config, dataset, importer, files_or_directories)

async def do_import(file_store: FileStore, uuid: UUID, import_config: ImportConfig, dataset: Dataset, importer: importers.Importer, files_or_directories: Iterable[str]):
    key_paths: Dict[str, Dict[Path, FileKey]] = {}
    for table_name, table in dataset.tables.items():
        key_paths[table_name] = {}
        table.add_flush_hook(move_files, file_store, import_config,key_paths[table_name])

    async def import_path(file_or_directory: Path):
        """Import a file or directory into the annex"""
        if file_or_directory.is_file():
            await import_file(file_or_directory)
        elif file_or_directory.is_dir():
            await import_directory(file_or_directory)
        else:
            raise ValueError(f"Path {file_or_directory} is not a file or directory")

    async def import_directory(path: Path):
        """Import a directory into the annex"""
        logger.debug(f"Importing directory {path}")
        for root, _, files in os.walk(path):
            root_path = Path(root)
            for file in files:
                await import_file(root_path / file)

    async def import_file(path: Path):
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
        key = import_config.file_key_type.from_file(path, importer.extension(path))

        if importer:
            key_columns = importer.key_columns(path)
            if key_columns:
                table_name = importer.table_name(path)
                table = dataset.get_table(table_name)
                await table.insert_file_source(key_columns, key, uuid)
                key_paths[table_name][path] = key
            if not key_columns:
                raise AnnexImportError("Importer did not produce a set of key columns, it is not safe to import")

    for file_or_directory in files_or_directories:
        await import_path(Path(file_or_directory))

async def move_files(file_store: FileStore, import_config: ImportConfig, files: Dict[Path, AnnexKey]):
    """Move files to the annex"""
    logger.debug("moving annex files")
    for file_path, key in files.items():
        if import_config.copy:
            await file_store.copy_file(file_path, key)
        else:
            await maybe_await(file_store.put_file(file_path, key))
        if import_config.move:
            # TODO: Add an extra check here that the file was added successfully, then delete the file
            # os.remove(file_path)
            pass
        elif import_config.symlink:
            # TODO: If the filestore doesn't support symlinks, that should have been caught earlier.
            # Otherwise, verify that the file was successfully moved, get the new path, and create the symlink
            pass
    
    files.clear()
