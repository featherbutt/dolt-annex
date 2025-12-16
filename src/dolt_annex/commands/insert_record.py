from typing import cast

from plumbum import cli # type: ignore

from dolt_annex.commands.import_command import move_files
from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.remote import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.application import Application
from dolt_annex.file_keys import get_file_key_type
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.table import Dataset

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
        default = "Sha256e",
    )

    extension = cli.SwitchAttr(
        "--extension",
        str,
        help="The file extension of the inserted record",
        default = "txt",
    )

    remote = cli.SwitchAttr(
        "--remote",
        str,
        help="If set, insert the record into the specified remote instead of the local annex",
    )
        
    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1
        base_config: Config = self.parent.config
        dataset_schema = DatasetSchema.must_load(self.dataset)

        file_key_type = get_file_key_type(self.file_key_type)
        BATCH_SIZE = 1000 # Arbitrary batch size for this command
        async with Dataset.connect(base_config, BATCH_SIZE, dataset_schema) as dataset:
            file_bytes = self.file_bytes.encode('utf-8')
            if self.extension == "":
                extension = None
            else:
                extension = self.extension
            key = file_key_type.from_bytes(file_bytes, extension)

            key_columns = cast(TableRow, self.key_columns.split(','))
            table = dataset.get_table(self.table_name)

            if self.remote:
                remote_repo = Repo.must_load(self.remote)
                remote_uuid = remote_repo.uuid
                filestore = ContentAddressableStorage.from_remote(remote_repo).file_store
                dataset.dolt.initialize_dataset_source(dataset_schema, remote_repo.uuid)
            else:
                remote_uuid = base_config.get_uuid()
                filestore = base_config.get_filestore()
            async with filestore.open(base_config):
                await table.insert_file_source(key_columns, key, remote_uuid)
            print(f"Inserted row ({', '.join(key_columns)}, {key}) into table '{self.table_name}' in dataset '{self.dataset}'")
        await maybe_await(filestore.put_file_bytes(file_bytes, key))
        return 0