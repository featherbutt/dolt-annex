import argparse
import asyncio
from typing import Optional

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.sftp import SftpFileStoreModel
from dolt_annex.gallery_dl_plugin import make_default_schema, run_gallery_dl
from dolt_annex.replicated_db.dolt import DatabaseConnection, Dataset
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.util.inquirerpy import Choice, choices, text, confirm, secret

from dolt_annex.interactive.ensure_config import ensure_config
from dolt_annex.interactive.ensure_database import ensure_database
from dolt_annex.interactive.ensure_filestore import ensure_filestore, ensure_repo
from dolt_annex.interactive.ensure_dolt_binary import ensure_dolt_binary
from dolt_annex.interactive.database_search import prompt_next_column

import logging

from dolt_annex.server import http
logger = logging.getLogger(__name__)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to the configuration file")
    parser.add_argument("--repo", help="Repository name", default="__local__")
    parser.add_argument("--dataset", help="Dataset name", default="gallery-dl")
    parser.add_argument("--dolt-binary", help="Path to the Dolt binary")
    parsed_args = parser.parse_args()
    config_file, repo_name, dataset_name, dolt_binary = parsed_args.config, parsed_args.repo, parsed_args.dataset, parsed_args.dolt_binary

    try:
        config = await ensure_config(config_file, repo_name)
        logging.basicConfig(level=config.log_level.upper())
        repo_model =await ensure_repo(config, repo_name)

        database_password: Optional[str]
        match config.dolt.connection.password:
            case str() as password:
                database_password = password
            case False:
                database_password = None
            case True:
                database_password = await secret("Enter the database password:")

        dataset_schema = ensure_dataset_schema(dataset_name)
        
        async with (
            Repo.open(config, repo_model) as repo,
            DatabaseConnection.open(config, database_password) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
            http.serve_filestore(repo.filestore.file_store) as http_server,
        ):
            await loop(config, repo, dataset, http_server)
    except Exception as e:
        print("An error occurred: %s", e)
        await text("Press Enter to exit...")
        

async def get_filestore_password(filestore_model: FileStoreModel, database_password_needed: bool) -> Optional[str]:
    match filestore_model:
        case SftpFileStoreModel():
            match filestore_model.connection.password:
                case str() as password:
                    return password
                case False:
                    return None

            if database_password_needed:
                if await confirm(
                    message="Reuse the database password for the filestore?",
                    default=True
                ):
                    return database_password
            return await text("Enter the filestore password:")
    return None

def ensure_dataset_schema(dataset_name: str) -> DatasetSchema:
    dataset_schema = DatasetSchema.load(dataset_name)
    if not dataset_schema:
        # Initialize the dataset if it doesn't exist
        logger.info("Dataset %s not found, creating with default schema.", dataset_name)
        dataset_schema = make_default_schema(dataset_name)
        dataset_schema.save()
    return dataset_schema

async def loop(config: Config, repo: Repo, dataset: Dataset, http_server: http.HTTPServer):
    # Command menu has command for gallery-dl download and db search. Selecting a result in the search displays the file.
        while True:
            try:
                func = await choices(
                    message="Select an action:",
                    choices=[
                        Choice(name="Gallery-dl Download", value=lambda: gallery_dl_download(config, repo, dataset)),
                        Choice(name="Database Search", value=lambda: database_search(repo, dataset, http_server)),
                        Choice(name="Exit", value=lambda: exit()),
                    ]
                )
                await func()
            except* Exception as e:
                for exc in e.exceptions:
                    print(f"Error: {exc}")

async def gallery_dl_download(config: Config, repo: Repo, dataset: Dataset):
    urls = (await text(
        message="Enter the URLs to download, separated by whitespace"
    )).split()
    await run_gallery_dl(config, repo, dataset, *urls)
    await dataset.conn.flush()

async def database_search(repo: Repo, dataset: Dataset, http_server: http.HTTPServer):
    async with (
        dataset.with_table("metadata") as table,
        table.with_repo(repo.uuid) as table_replica,
        dataset.with_table("submissions") as submission_table,
        submission_table.with_repo(repo.uuid) as submission_table_replica,
    ):
        table_row = await prompt_next_column(table_replica, TableRow({}))
        table_row["metadata_file_key"] = table_row.pop("file_key")
        submission_table_rows = submission_table_replica.get_rows(columns=[submission_table_replica.schema.file_column], filters=TableFilter.from_table_row(table_row))
        for row in submission_table_rows:
            await http_server.display_file(FileKey.must_parse(row[submission_table_replica.schema.file_column]))

__all__ = ["ensure_config", "ensure_dolt_binary", "ensure_database", "ensure_filestore", "ensure_repo"]

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")