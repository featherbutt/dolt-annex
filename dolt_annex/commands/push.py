#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID
from pathlib import Path

from typing_extensions import List, Iterable, Optional, Tuple

from plumbum import cli # type: ignore

from dolt_annex.application import Application, Downloader
from dolt_annex.config import get_config
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.table import FileTable
from dolt_annex.filestore import get_old_relative_annex_key_path, get_key_path
from dolt_annex.datatypes import AnnexKey, FileTableSchema, Remote, TableRow
from dolt_annex.logger import logger
from dolt_annex.commands.sync import SshSettings, TableFilter, file_mover, FileMover, diff_query

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

    table = cli.SwitchAttr(
        "--table",
        str,
        help="The name of the table being pushed",
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
        """Entrypoint for push command"""
        table = FileTableSchema.from_name(self.table)
        if not table:
            logger.error(f"Table {self.table} not found")
            return 1
        remote_name = self.remote or self.parent.config.dolt_remote
        remote = Remote.from_name(remote_name)
        if not remote:
            logger.error(f"Remote {remote_name} not found")
            return 1
        with Downloader(self.parent.config, self.batch_size, table) as downloader:
            ssh_settings = SshSettings(Path(self.ssh_config), Path(self.known_hosts))

            do_push(downloader, remote, ssh_settings, self.table, self.filters, self.limit)
        return 0

def do_push(downloader: FileTable, file_remote: Remote, ssh_settings: SshSettings, file_key_table: FileTableSchema, where: List[TableFilter], limit: Optional[int] = None) -> List[AnnexKey]:
    dolt = downloader.dolt
    remote_uuid = file_remote.uuid
    local_uuid = get_config().local_uuid

    # TODO: Dolt remote not necessarily the same as file remote, know when pull is necessary
    # dolt.pull_branch(remote_uuid, dolt_remote)

    with file_mover(file_remote, ssh_settings) as mover:
        total_files_pushed: List[AnnexKey] = []
        while True:
            keys_and_submissions = list(diff_keys(dolt, str(local_uuid), str(remote_uuid), file_key_table, where, limit))
            has_more = push_submissions_and_keys(keys_and_submissions, downloader, mover, remote_uuid, total_files_pushed)
            if not has_more:
                break
    downloader.flush()

    return total_files_pushed

def push_submissions_and_keys(keys_and_submissions: Iterable[Tuple[AnnexKey, TableRow]], downloader: FileTable, mover: FileMover, remote_uuid: UUID, files_pushed: List[AnnexKey]) -> bool:
    has_more = False
    for key, submission in keys_and_submissions:
        has_more = True
        logger.info(f"pushing {submission}: {key}")
        rel_key_path = get_key_path(key)
        old_rel_key_path = get_old_relative_annex_key_path(key)
        if not mover.put(old_rel_key_path, rel_key_path):
            mover.put(rel_key_path, rel_key_path)
        downloader.insert_file_source(submission, key, remote_uuid)
        files_pushed.append(key)
    downloader.flush()
    return has_more

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, file_key_table: FileTableSchema, filters: List[TableFilter], limit = None) -> Iterable[Tuple[AnnexKey, TableRow]]:
    refs = [in_ref, not_in_ref]
    refs.sort()
    union_branch_name = f"union-{refs[0]}-{refs[1]}-{file_key_table.name}"
    
    in_ref_branch = f"{in_ref}-{file_key_table.name}"
    not_in_ref_branch = f"{not_in_ref}-{file_key_table.name}"
    # Create the union branch if it doesn't exist
    with dolt.maybe_create_branch(union_branch_name, in_ref_branch):
        dolt.merge(in_ref_branch)
        dolt.merge(not_in_ref_branch)
        query = diff_query(file_key_table, filters)
        if limit is not None:
            query += " LIMIT %s"
            query_results = dolt.query(query, (not_in_ref_branch, union_branch_name, limit))
        else:
            query_results = dolt.query(query, (not_in_ref_branch, union_branch_name))
        for (annex_key, _, *key_parts) in query_results:
            yield (AnnexKey(annex_key), TableRow(key_parts))