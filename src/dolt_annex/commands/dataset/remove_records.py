from typing_extensions import List

from plumbum import cli

from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.application import Application
from dolt_annex.replicated_db.dolt import DatabaseConnection

class RemoveRecords(cli.Application):
    """Remove all records matching a filter from a dataset. Does not remove files from the filestore."""

    parent: Application

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The name of the repo being removed from. If not specified, uses the local repo.",
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being removed from",
        mandatory = True
    )

    table_name = cli.SwitchAttr(
        "--table-name",
        str,
        help="The name of the table being removed from",
        mandatory = True
    )

    filters: List[TableFilter] = []

    @cli.switch(
        "--where",
        str,
        list = True,
        help="A filter condition on the table rows to be removed",
    )
    def where(self, filter_strings: List[str]):
        for filter_string in filter_strings:
            if '=' not in filter_string:
                raise ValueError(f"Invalid filter string: {filter_string}")
            column_name, column_value = filter_string.split('=', maxsplit=1)
            self.filters.append(TableFilter(column_name, column_value))

    def __init__(self, *args, **kwargs):
        self.filters = []
        super().__init__(*args, **kwargs)

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
            table.remove_rows(filters=self.filters)
            print(f"Removed row(s) from table '{self.table_name}' in dataset '{self.dataset}'")
        return 0
