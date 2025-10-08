#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from uuid import UUID

from typing_extensions import Iterable, Optional, Tuple, List

from plumbum import cli # type: ignore

from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.table import Dataset, FileTable
from dolt_annex.commands.sync import SshSettings, TableFilter
from dolt_annex.application import Application, Downloader
from dolt_annex.commands.push import FileMover, file_mover, diff_keys
from dolt_annex.filestore import get_old_relative_annex_key_path, get_key_path
from dolt_annex.logger import logger
from dolt_annex.datatypes import AnnexKey, TableRow, Repo
from dolt_annex import context

class Pull(cli.Application):
    """Pull imported files from a remote repository"""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
        default = 1000,
    )

    ssh_config = cli.SwitchAttr(
        "--ssh-config",
        str,
        help="The path to the ssh config file",
        default = "~/.ssh/config",
    )

    known_hosts = cli.SwitchAttr(
        "--known-hosts",
        str,
        help="The path to the known hosts file",
        default = None,
    )

    limit = cli.SwitchAttr(
        "--limit",
        int,
        help="The maximum number of files to pull",
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
        help="The name of the dataset being pulled",
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

    def main(self, *args) -> int:
        """Entrypoint for pull command"""
        dataset = DatasetSchema.must_load(self.dataset)
        remote_name = self.remote or self.parent.config.dolt_remote
        remote = Repo.must_load(remote_name)
        ssh_settings = SshSettings(Path(self.ssh_config), Path(self.known_hosts))

        with Downloader(self.parent.config, self.batch_size, dataset) as downloader:
            pull_dataset(downloader, remote, ssh_settings, self.filters, self.limit)
        return 0
    
def pull_submissions_and_keys(keys_and_submissions: Iterable[Tuple[AnnexKey, TableRow]], downloader: FileTable, mover: FileMover, local_uuid: UUID, files_pulled: List[AnnexKey]) -> bool:
    has_more = False
    for key, table_row in keys_and_submissions:
        has_more = True
        logger.info(f"pulling {table_row}: {key}")
        rel_key_path = get_key_path(key)
        old_rel_key_path = get_old_relative_annex_key_path(key)
        if not mover.get(old_rel_key_path, rel_key_path):
            mover.get(rel_key_path, rel_key_path)
        downloader.insert_file_source(table_row, key, local_uuid)
        files_pulled.append(key)
    downloader.flush()
    return has_more

def pull_dataset(dataset: Dataset, file_remote: Repo, ssh_settings: SshSettings, where: List[TableFilter], limit: Optional[int] = None, out_pulled_keys: Optional[List[AnnexKey]] = None) -> List[AnnexKey]:
    if out_pulled_keys is None:
        out_pulled_keys = []
    dataset.pull_from(file_remote)
    for table in dataset.tables.values():
        pull_table(table, file_remote, ssh_settings, where, limit, out_pulled_keys)
    return out_pulled_keys

def pull_table(table: FileTable, file_remote: Repo, ssh_settings: SshSettings, where: List[TableFilter], limit: Optional[int] = None, out_pulled_keys: Optional[List[AnnexKey]] = None) -> List[AnnexKey]:
    if out_pulled_keys is None:
        out_pulled_keys = []
    dolt = table.dolt
    local_uuid = context.local_uuid.get()
    remote_uuid = file_remote.uuid

    with file_mover(file_remote, ssh_settings) as mover:
        while True:
            keys_and_submissions = list(diff_keys(dolt, str(remote_uuid), str(local_uuid), table.schema, where, limit))
            has_more = pull_submissions_and_keys(keys_and_submissions, table, mover, local_uuid, out_pulled_keys)
            if not has_more:
                break
    return out_pulled_keys

