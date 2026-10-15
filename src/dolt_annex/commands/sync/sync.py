#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

from typing_extensions import List, Optional, Tuple

from plumbum import cli

from dolt_annex.commands.application import Application
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.config.base import Config
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.datatypes.repo import Repo
from dolt_annex.sync import move_dataset

class Sync(cli.Application):
    """Sync imported files from one repository to another"""

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
        default = None,
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
        help="The maximum number of files to sync",
        default = None,
    )

    from_repo_name = cli.SwitchAttr(
        "--from",
        str,
        help="The name of the repo to copy files from. Defaults to the default repo",
    )

    to_repo_name = cli.SwitchAttr(
        "--to",
        str,
        help="The name of the repo to copy files to. Defaults to the default repo",
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being synced",
    )

    ignore_missing = cli.Flag(
        "--ignore-missing",
        help="Ignore missing files when syncing",
    )

    @cli.switch(
        "--where",
        str,
        list = True,
        help="A filter condition on the table rows to be synced",
    )
    def where(self, filter_strings: List[str]):
        for filter_string in filter_strings:
            if '=' not in filter_string:
                raise ValueError(f"Invalid filter string: {filter_string}")
            column_name, column_value = filter_string.split('=', maxsplit=1)
            self.filters.append(TableFilter(column_name, column_value))

    filters: List[TableFilter] = []

    def report(self, files_moved: int, from_name: Optional[str], to_name: Optional[str]) -> None:
        print(f"Synced {files_moved} files from {from_name or 'default repo'} to {to_name or 'default repo'}")

    async def main(self, *args: list[str]) -> int:
        """Entrypoint for sync command"""
        base_config: Config = self.parent.config
        if self.ssh_config:
            base_config.ssh.ssh_config = Path(self.ssh_config)
        if self.known_hosts:
            base_config.ssh.known_hosts = Path(self.known_hosts)

        if self.from_repo_name == self.to_repo_name:
            print("The source and destination repos must be different. Use --from and/or --to to set them.")
            return 1

        dataset_schema = DatasetSchema.must_load(self.dataset)

        async with (
            Repo.open(base_config, self.from_repo_name,) as from_repo,
            Repo.open(base_config, self.to_repo_name) as to_repo,
            DatabaseConnection.open(base_config) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
        ):
            files_moved = await move_dataset(dataset, from_repo, to_repo, self.filters, self.limit, self.ignore_missing)
            self.report(files_moved, self.from_repo_name, self.to_repo_name)
        return 0
