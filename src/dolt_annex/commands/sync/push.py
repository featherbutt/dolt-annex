#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from pathlib import Path
from uuid import UUID

from typing_extensions import List, Optional

from plumbum import cli

from dolt_annex.application import Application
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.table import Dataset, TableFilter
from dolt_annex.datatypes import AnnexKey
from dolt_annex.datatypes.remote import Repo
from dolt_annex.sync import move_table

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

    async def main(self, *args) -> int:
        """Entrypoint for push command"""
        base_config = self.parent.config

        if self.ssh_config:
            base_config.ssh.ssh_config = Path(self.ssh_config)
        if self.known_hosts:
            base_config.ssh.known_hosts = Path(self.known_hosts)

        dataset_schema = DatasetSchema.must_load(self.dataset)
        remote_name = self.remote or self.parent.config.dolt.default_remote
        remote_repo = Repo.must_load(remote_name)
        remote_file_store = ContentAddressableStorage.from_remote(remote_repo).file_store
        local_file_store = self.parent.config.filestore
        if not local_file_store:
            raise ValueError("No local filestore configured")
        local_uuid = self.parent.config.get_uuid()

        async with (
            local_file_store.open(base_config),
            remote_file_store.open(base_config),
            Dataset.connect(self.parent.config, self.batch_size, dataset_schema) as dataset
        ):
            # TODO: This is a really hacky way to create the branch being pushed to if it doesn't exist.
            dataset.dolt.initialize_dataset_source(dataset_schema, remote_repo.uuid)
            pushed_files = await push_dataset(dataset, local_uuid, remote_repo, remote_file_store, local_file_store, self.filters, self.limit)
            print(f"Pushed {len(pushed_files)} files to remote {remote_name}")
        return 0

async def push_dataset(dataset: Dataset, local_uuid: UUID, remote_repo: Repo, remote_file_store: FileStore, local_file_store: FileStore, where: List[TableFilter], limit: Optional[int] = None, out_pushed_files: Optional[List[AnnexKey]] = None) -> List[AnnexKey]:
    if out_pushed_files is None:
        out_pushed_files = []
    # TODO: Separate the concept of a Dolt remote from a Dolt-annex remote.
    # There may not be A Dolt remote to pull from
    # dataset.pull_from(remote_repo)
    for table in dataset.tables.values():
        await move_table(table, local_uuid, remote_repo.uuid, local_file_store, remote_file_store, where, False, limit, out_pushed_files)
    return out_pushed_files
