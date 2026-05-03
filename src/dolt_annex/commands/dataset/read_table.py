import json
import sys

from typing_extensions import List

from plumbum import cli # type: ignore

from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.application import Application
from dolt_annex.replicated_db.dolt import DatabaseConnection

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

    columns = cli.SwitchAttr(
        "--columns",
        str,
        list = True,
        help="The columns to read. If not specified, reads all columns.",
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
            repo = RepoModel.must_load(self.repo)
        else:
            repo = base_config.get_default_repo()
        dataset_schema = DatasetSchema.must_load(self.dataset)

        async with (
            as_acm(DatabaseConnection.open(base_config)) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
            dataset.with_repo(repo.uuid) as repo_dataset,
        ):
            table = repo_dataset.get_table(self.table_name)
            for row in table.get_rows(columns=self.columns, filters=self.filters):
                json.dump(row, sys.stdout)
                sys.stdout.write('\n')

        return 0