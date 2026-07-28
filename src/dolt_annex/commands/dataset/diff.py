#!/usr/bin/env python
# -*- coding: utf-8 -*-


from typing_extensions import List

from plumbum import cli

from dolt_annex.application import Application
from dolt_annex.commands import SubCommand
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.datatypes.repo import RepoModel
class Diff(SubCommand):
    """Print records that differ between two versions of a dataset."""

    limit = cli.SwitchAttr(
        "--limit",
        int,
        help="The maximum number of records to print",
        default = None,
    )

    from_repo = cli.SwitchAttr(
        "--from",
        str,
        help="The first repo to compare",
    )

    to_repo = cli.SwitchAttr(
        "--to",
        str,
        help="The second repo to compare",
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being compared",
    )

    table_name = cli.SwitchAttr(
        "--table",
        str,
        help="The name of the table in the dataset being compared",
        mandatory=True,
    )

    @cli.switch(
        "--where",
        str,
        list = True,
        help="A filter condition on the table rows to be pushed",
    )
    def where(self, filter_strings: List[str]):
        for filter_string in filter_strings:
            if '=' not in filter_string:
                raise ValueError(f"Invalid filter string: {filter_string}")
            column_name, column_value = filter_string.split('=', maxsplit=1)
            self.filters.append(TableFilter(column_name, column_value))

    filters: List[TableFilter] = []

    async def main(self, *args: list[str]) -> int:
        """Entrypoint for diff command"""
        base_config: Config = self.config

        dataset_schema = DatasetSchema.must_load(self.dataset)
        with (
            DatabaseConnection.open(base_config) as conn,
            conn.open_dataset(dataset_schema) as dataset,
        ):
            local_repo_model = RepoModel.must_load(self.from_repo)
            remote_repo_model = RepoModel.must_load(self.to_repo)
            table_schema = dataset_schema.get_table(self.table_name)
            async with dataset.with_table(self.table_name) as replicated_table:
                keys_and_submissions = list(replicated_table.diff_keys(local_repo_model.uuid, remote_repo_model.uuid, self.filters, self.limit))
                for diff_type, key, to_submission, from_submission in keys_and_submissions:
                        # TODO: Display removed rows
                        print(",".join([diff_type, str(key), str(to_submission)]))
        return 0