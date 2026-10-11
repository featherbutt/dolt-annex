from dolt_annex.interactive.ensure_config import ensure_config
from dolt_annex.interactive.ensure_database import ensure_database
from dolt_annex.interactive.ensure_filestore import ensure_filestore, ensure_repo
from dolt_annex.interactive.ensure_dolt_binary import ensure_dolt_binary

import argparse
import asyncio
from typing import Optional

from InquirerPy.inquirer import text, secret, confirm, rawlist # pyright: ignore[reportPrivateImportUsage]

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.errors import DoltAnnexApplicationError
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.sftp import SftpFileStoreModel
from dolt_annex.gallery_dl_plugin import make_default_schema, run_gallery_dl
from dolt_annex.replicated_db.dolt import DatabaseConnection, Dataset

import logging
logger = logging.getLogger(__name__)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to the configuration file")
    parser.add_argument("--repo", help="Repository name", default="__local__")
    parser.add_argument("--dataset", help="Dataset name", default="gallery-dl")
    parser.add_argument("--dolt-binary", help="Path to the Dolt binary")
    parsed_args = parser.parse_args()
    config_file, repo_name, dataset_name, dolt_binary = parsed_args.config, parsed_args.repo, parsed_args.dataset, parsed_args.dolt_binary

    config = await ensure_config(config_file, repo_name)
    await ensure_repo(config, repo_name)

    database_password: Optional[str]
    match config.dolt.connection.password:
        case str() as password:
            database_password = password
        case False:
            database_password = None
        case True:
            database_password = await secret(
                message="Enter the database password:"
            ).execute_async()

    dataset_schema = ensure_dataset_schema(dataset_name)
    repo_model = await ensure_repo(config, repo_name)

    async with (
        Repo.open(config, repo_model) as repo,
        DatabaseConnection.open(config, database_password) as conn,
        as_acm(conn.open_dataset(dataset_schema)) as dataset,
    ):
        await loop(config, repo, dataset)

def get_filestore_password(filestore_model: FileStoreModel, database_password_needed: bool) -> Optional[str]:
    match filestore_model:
        case SftpFileStoreModel():
            match filestore_model.connection.password:
                case str() as password:
                    return password
                case False:
                    return None

            if database_password_needed:
                if confirm(
                    message="Reuse the database password for the filestore?",
                    default=True
                ).execute():
                    return database_password
            return text(
                message="Enter the filestore password:"
            ).execute()
    return None

def ensure_dataset_schema(dataset_name: str) -> DatasetSchema:
    dataset_schema = DatasetSchema.load(dataset_name)
    if not dataset_schema:
        # Initialize the dataset if it doesn't exist
        logger.info("Dataset %s not found, creating with default schema.", dataset_name)
        dataset_schema = make_default_schema(dataset_name)
        dataset_schema.save()
    return dataset_schema

async def loop(config: Config, repo: Repo, dataset: Dataset):
    # Command menu has command for gallery-dl download and db search. Selecting a result in the search displays the file.
    
        while True:
            try:
                action = await rawlist(
                    message="Select an action:",
                    choices=["Gallery-dl Download", "Database Search", "Exit"]
                ).execute_async()
                match action:
                    case "Gallery-dl Download":
                        urls = (await text(
                            message="Enter the URLs to download, separated by whitespace"
                        ).execute_async()).split()
                        await run_gallery_dl(repo, config.gallery_dl, dataset, *urls)
                    case "Database Search":
                        # Implement database search logic here
                        pass
                    case "Exit":
                        break
            except* Exception as e:
                for exc in e.exceptions:
                    print(f"Error: {exc}")

__all__ = ["ensure_config", "ensure_dolt_binary", "ensure_database", "ensure_filestore", "ensure_repo"]

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")