from typing_extensions import cast

from plumbum import cli # type: ignore

from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.application import Application
from dolt_annex.file_keys import get_file_key_type
from dolt_annex.table import DatabaseConnection, Dataset

class InsertRecord(cli.Application):
    """Insert a single record into the annex and database. Primarily used for testing."""

    parent: Application

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being imported to",
        mandatory = True
    )

    table_name = cli.SwitchAttr(
        "--table-name",
        str,
        help="The name of the table being imported to",
        mandatory = True
    )

    key_columns = cli.SwitchAttr(
        "--key-columns",
        str,
        help="The name of the dataset being imported to",
        mandatory = True
    )

    file_bytes = cli.SwitchAttr(
        "--file-bytes",
        str,
        help="The bytes of the file to insert",
        mandatory = True
    )

    file_key_type = cli.SwitchAttr(
        "--file-key-type",
        str,
        help="The type of file key to use",
        default = "SHA256E",
    )

    extension = cli.SwitchAttr(
        "--extension",
        str,
        help="The file extension of the inserted record",
        default = "txt",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, insert the record into the specified repo instead of the local annex",
    )
        
    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1
        base_config: Config = self.parent.config
        dataset_schema = DatasetSchema.must_load(self.dataset)

        file_key_type = get_file_key_type(self.file_key_type)
        
        async with (
            Repo.open(base_config, self.repo) as repo,
            as_acm(DatabaseConnection.connect(base_config)) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
            dataset.with_repo(repo.uuid) as repo_dataset,
        ):
            file_bytes = self.file_bytes.encode('utf-8')
            if self.extension == "":
                extension = None
            else:
                extension = self.extension
            key = file_key_type.from_bytes(file_bytes, extension)

            key_columns = cast(TableRow, self.key_columns.split(','))
            table = repo_dataset.get_table(self.table_name)

            await table.insert_file_source(key_columns, key)
            await maybe_await(repo.filestore.put_file_bytes(file_bytes, key))
            print(f"Inserted row ({', '.join(key_columns)}, {key}) into table '{self.table_name}' in dataset '{self.dataset}'")
        return 0