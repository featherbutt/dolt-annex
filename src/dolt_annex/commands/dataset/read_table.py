from typing import List

from plumbum import cli # type: ignore

from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.application import Application
from dolt_annex.table import Dataset, TableFilter

class ReadTable(cli.Application):
    """Read rows from a dataset table for a specific remote. Primarily used for testing."""

    parent: Application

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The name of the repo being read from. If not specified, uses the local repo.",
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being read from",
        mandatory = True
    )

    table_name = cli.SwitchAttr(
        "--table-name",
        str,
        help="The name of the table in the dataset being read from",
        mandatory = True
    )

    filters: List[TableFilter] = []

    @cli.switch(
        "--where",
        str,
        list = True,
        help="A filter condition on the table rows to be read",
    )
    def where(self, filter_strings: List[str]):
        for filter_string in filter_strings:
            if '=' not in filter_string:
                raise ValueError(f"Invalid filter string: {filter_string}")
            column_name, column_value = filter_string.split('=', maxsplit=1)
            self.filters.append(TableFilter(column_name, column_value))

        
    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1
        base_config: Config = self.parent.config
        if self.repo:
            repo = Repo.must_load(self.repo)
            uuid = repo.uuid
        else:
            uuid = base_config.get_uuid()
        dataset_schema = DatasetSchema.must_load(self.dataset)

        BATCH_SIZE = 1000 # Arbitrary batch size for this command
        async with Dataset.connect(base_config, BATCH_SIZE, dataset_schema) as dataset:
            table = dataset.get_table(self.table_name)
            for row in table.get_rows(uuid, self.filters):
                print(", ".join(str(cell) for cell in row))

        return 0