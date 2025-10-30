#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID
import sys
from pathlib import Path

from typing_extensions import List, Iterable, Optional, Tuple

from plumbum import cli # type: ignore

from dolt_annex.application import Application
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.sync import FileModifiedError, SyncResults, diff_query
from dolt_annex.table import Dataset, FileTable
from dolt_annex.datatypes import AnnexKey, TableRow
from dolt_annex.datatypes.table import FileTableSchema
from dolt_annex.datatypes.remote import Repo


class Sync(cli.Application):
    """Push and pull imported files to and from a remote repository"""

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

    table = cli.SwitchAttr(
        "--table",
        str,
        help="The name of the table being synced",
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

    def main(self, *args) -> int:
        """Entrypoint for sync command"""
        if len(args) > 0:
            print(f"Unexpected positional arguments provided to {sys.argv[0]} sync")
        table = FileTableSchema.must_load(self.table)
        remote_name = self.remote or self.parent.config.dolt_remote
        remote = Repo.must_load(remote_name)
        with Dataset.connect(self.parent.config, table, self.batch_size) as dataset:
            ssh_settings = SshSettings(Path(self.ssh_config), Path(self.known_hosts))
            sync_dataset(dataset, remote, ssh_settings, self.table, self.filters, self.limit)
        return 0

def sync_dataset(dataset: Dataset, file_remote: Repo, ssh_settings: SshSettings, file_key_table: FileTableSchema, where: List[TableFilter], diff_type: str = "", limit: Optional[int] = None, sync_results: Optional[SyncResults] = None) -> SyncResults:
    if sync_results is None:
        sync_results = SyncResults()
    dataset.pull_from(file_remote)
    for table in dataset.tables.values():
        sync_table(table, file_remote, ssh_settings, file_key_table, where, diff_type, limit, sync_results)
    return sync_results

def sync_table(table: FileTable, file_remote: Repo, ssh_settings: SshSettings, file_key_table: FileTableSchema, where: List[TableFilter], diff_type: str = "", limit: Optional[int] = None, sync_results: Optional[SyncResults] = None) -> SyncResults:
    dolt = table.dolt
    remote_uuid = file_remote.uuid
    local_uuid = get_config().local_uuid

    with file_mover(file_remote, ssh_settings) as mover:
        total_files_synced = SyncResults()
        while True:
            keys_and_submissions = diff_keys(dolt, str(local_uuid), str(remote_uuid), file_key_table, where, limit)
            has_more = sync_keys(keys_and_submissions, table, mover, remote_uuid, total_files_synced)
            if not has_more:
                break
    table.flush()

    return total_files_synced

def sync_keys(keys: Iterable[Tuple[AnnexKey, str, TableRow]], downloader: FileTable, mover: FileMover, remote_uuid: UUID, files_synced: SyncResults) -> bool:
    has_more = False
    for key, diff_type, table_row in keys:
        has_more = True
        rel_key_path = get_key_path(key)
        match diff_type:
            case 'added':
                old_rel_key_path = get_old_relative_annex_key_path(key)
                if not mover.put(old_rel_key_path, rel_key_path):
                    mover.put(rel_key_path, rel_key_path)
            case 'removed':
                if not mover.get(rel_key_path, rel_key_path):
                    old_rel_key_path = get_old_relative_annex_key_path(key)
                    mover.get(old_rel_key_path, rel_key_path)
            case 'modified':
                raise FileModifiedError(key)
            case _:
                raise ValueError(f"Unknown diff type returned: {diff_type}")
        downloader.insert_file_source(table_row, key, remote_uuid)
        files_synced.files_pushed.append(key)
    downloader.flush()
    return has_more

def pull_personal_branch(dolt: DoltSqlServer, remote: Repo) -> None:
    """Fetch the personal branch for the remote"""
    dolt.pull_branch(str(remote.uuid), remote)

def diff_keys(dolt: DoltSqlServer, local_ref: str, remote_ref: str, file_key_table: FileTableSchema, filters: List[TableFilter], limit = None) -> Iterable[Tuple[AnnexKey, str, TableRow]]:
    query = diff_query(file_key_table, filters)
    
    if limit is not None:
        query += " LIMIT %s"
        query_results = dolt.query(query, (remote_ref, local_ref, limit))
    else:
        query_results = dolt.query(query, (remote_ref, local_ref))
    for (annex_key, diff_type, *key_parts) in query_results:
        yield (AnnexKey(annex_key), diff_type, TableRow(*key_parts))
