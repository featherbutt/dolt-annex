#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

from typing_extensions import List

from plumbum import cli

from dolt_annex.application import Application
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.table import Dataset, TableFilter
from dolt_annex.datatypes.repo import Repo
from dolt_annex.sync import move_dataset

class Push(cli.Application):
    """Push imported files to a remote repository"""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
        default = 1000,
    )

    ssh_config = cli.SwitchAttr(
        "--ssh-config",
        cli.ExistingFile,
        help="The path to the ssh config file",
        default = "~/.ssh/config",
    )

    known_hosts = cli.SwitchAttr(
        "--known-hosts",
        cli.ExistingFile,
        help="The path to the known hosts file",
        default = None,
    )

    limit = cli.SwitchAttr(
        "--limit",
        int,
        help="The maximum number of files to push",
        default = None,
    )

    remote = cli.SwitchAttr(
        "--remote",
        str,
        help="The name of the dolt-annex remote",
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being pushed",
    )

    ignore_missing = cli.Flag(
        "--ignore-missing",
        help="Ignore missing files when pulling",
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
        """Entrypoint for pull command"""
        base_config: Config = self.parent.config
        if self.ssh_config:
            base_config.ssh.ssh_config = Path(self.ssh_config)
        if self.known_hosts:
            base_config.ssh.known_hosts = Path(self.known_hosts)

        dataset_schema = DatasetSchema.must_load(self.dataset)
        remote_name = self.remote or base_config.dolt.default_remote

        async with (
            base_config.open_default_repo() as local_repo,
            Repo.open(base_config, remote_name) as remote_repo,
            Dataset.connect(self.parent.config, self.batch_size, dataset_schema) as dataset
        ):
            dataset.dolt.initialize_dataset_source(dataset_schema, remote_repo.uuid)
            pushed_files = await move_dataset(dataset, local_repo, remote_repo, self.filters, self.limit, None, self.ignore_missing)
            print(f"Pushed {len(pushed_files)} files to remote {remote_name}")
        return 0