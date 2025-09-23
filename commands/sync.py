#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from dataclasses import dataclass, field
import os
from uuid import UUID
import getpass
import sys
from pathlib import Path

from typing_extensions import List, Iterable, Optional, Generator, Tuple, Any

import sftpretty # type: ignore
from plumbum import cli # type: ignore

from annex import SubmissionId
from application import Application, Downloader
from config import get_config
from dolt import DoltSqlServer
from annex import AnnexCache
from git import get_old_relative_annex_key_path, get_key_path
import move_functions
from move_functions import MoveFunction
from remote import Remote
from tables import FileKeyTable
from type_hints import AnnexKey, TableRow
from logger import logger

@dataclass
class TableFilter:
    column_name: str
    column_value: Any

@dataclass
class SshSettings:
    ssh_config: Path
    known_hosts: Optional[Path]

@dataclass
class SyncResults:
    files_pushed: List[AnnexKey] = field(default_factory=list)
    files_pulled: List[AnnexKey] = field(default_factory=list)

    def __iadd__(self, other: 'SyncResults') -> 'SyncResults':
        self.files_pushed += other.files_pushed
        self.files_pulled += other.files_pulled
        return self
    
    def __bool__(self) -> bool:
        return bool(self.files_pushed or self.files_pulled)
    
class FileModifiedError(Exception):
    def __init__(self, key: AnnexKey):
        self.key = key
        super().__init__(f"File with annex key {key} has different content on both remotes")

class FileMover:
    local_cwd: Path
    remote_cwd: Path
    put_function: MoveFunction
    get_function: MoveFunction

    def __init__(self, put_function: MoveFunction, get_function: MoveFunction, remote_cwd: str, local_cwd = None) -> None:
        if local_cwd is None:
            local_cwd = os.getcwd()
        self.local_cwd = Path(local_cwd)
        self.remote_cwd = Path(remote_cwd)
        self.put_function = put_function
        self.get_function = get_function

    def put(self, local_path: Path, remote_path: Path) -> bool:
        """Move a file from the local filesystem to the remote filesystem"""
        abs_local_path = self.local_cwd / local_path
        abs_remote_path = self.remote_cwd / remote_path
        logger.info(f"Moving {abs_local_path} to {abs_remote_path}")
        return self.put_function(
            abs_local_path,
            abs_remote_path)
        
    def get(self, local_path: Path, remote_path: Path) -> bool:
        """Move a file from the local filesystem to the remote filesystem"""
        abs_local_path = self.local_cwd / local_path
        abs_remote_path = self.remote_cwd / remote_path
        logger.info(f"Moving {abs_remote_path} to {abs_local_path}")
        return self.get_function(
            abs_remote_path,
            abs_local_path)

    @contextmanager
    def cd(self, local_path: Optional[str] = None, remote_path: Optional[str] = None):
        """Change the remote directory"""
        old_local_cwd = self.local_cwd
        old_remote_cwd = self.remote_cwd
        if local_path is not None:
            self.local_cwd = self.local_cwd / local_path
        if remote_path is not None:
            self.remote_cwd = self.remote_cwd / remote_path
        yield
        self.local_cwd = old_local_cwd
        self.remote_cwd = old_remote_cwd

@contextmanager
def file_mover(remote: Remote, ssh_settings: SshSettings) -> Generator[FileMover, None, None]:
    base_config = get_config()
    local_path = os.path.abspath(base_config.files_dir)
    if '@' in remote.url:
        user, rest = remote.url.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)

        cnopts = sftpretty.CnOpts(config=ssh_settings.ssh_config, knownhosts=ssh_settings.known_hosts)
        cnopts.log_level = 'error'

        extra_opts = {}
        if base_config.encrypted_ssh_key:
            extra_opts["private_key_pass"] = getpass.getpass("Enter passphrase for private key: ")

        with sftpretty.Connection(host, cnopts=cnopts, username = user, default_path = path, **extra_opts) as sftp:
            def sftp_put(
                local_path: Path,
                remote_path: Path,
            ) -> bool:
                """Move a file from the local filesystem to the remote filesystem using SFTP"""
                if not local_path.exists():
                    return False
                sftp.mkdir_p(remote_path.parent.as_posix())
                if sftp.exists(remote_path.as_posix()):
                    logger.info(f"File {remote_path} already exists, skipping")
                    return True
                sftp.put(local_path.as_posix(), remote_path.as_posix())
                return True
            def sftp_get(
                remote_path: Path,
                local_path: Path,
            ) -> bool:
                """Move a file from the remote filesystem to the local filesystem using SFTP"""
                local_path.parent.mkdir(parents=True, exist_ok=True)
                if not sftp.exists(remote_path.as_posix()):
                    return False
                if local_path.exists():
                    logger.info(f"File {local_path} already exists, skipping")
                    return True
                sftp.get(remote_path.as_posix(), local_path.as_posix())
                return True
            yield FileMover(sftp_put, sftp_get, sftp.getcwd(), local_path)
    elif remote.url.startswith("file://"):
        # Remote path may be relative to the local git directory
        yield FileMover(move_functions.copy, move_functions.copy, remote.url[7:], local_path)
    else:
        raise ValueError(f"Unknown remote URL format: {remote.url}")
    


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
        table = FileKeyTable.from_name(self.table)
        if not table:
            logger.error(f"Table {self.table} not found")
            return 1
        remote_name = self.remote or self.parent.config.dolt_remote
        remote = Remote.from_name(remote_name)
        if not remote:
            logger.error(f"Remote {remote_name} not found")
            return 1
        with Downloader(self.parent.config, table, self.batch_size) as downloader:
            ssh_settings = SshSettings(Path(self.ssh_config), Path(self.known_hosts))
            do_sync(downloader, remote, ssh_settings, self.table, self.filters, self.limit)
        return 0

def do_sync(downloader: AnnexCache, file_remote: Remote, ssh_settings: SshSettings, file_key_table: FileKeyTable, where: List[TableFilter], diff_type: str = "", limit: Optional[int] = None) -> SyncResults:
    dolt = downloader.dolt
    remote_uuid = file_remote.uuid
    local_uuid = get_config().local_uuid

    with file_mover(file_remote, ssh_settings) as mover:
        total_files_synced = SyncResults()
        while True:
            keys_and_submissions = diff_keys(dolt, str(local_uuid), str(remote_uuid), file_key_table, where, limit)
            has_more = sync_keys(keys_and_submissions, downloader, mover, remote_uuid, total_files_synced)
            if not has_more:
                break
    downloader.flush()

    return total_files_synced

def sync_keys(keys: Iterable[Tuple[AnnexKey, str, TableRow]], downloader: AnnexCache, mover: FileMover, remote_uuid: UUID, files_synced: SyncResults) -> bool:
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

def pull_personal_branch(dolt: DoltSqlServer, remote: Remote) -> None:
    """Fetch the personal branch for the remote"""
    dolt.pull_branch(str(remote.uuid), remote)

def diff_keys(dolt: DoltSqlServer, local_ref: str, remote_ref: str, file_key_table: FileKeyTable, filters: List[TableFilter], limit = None) -> Iterable[Tuple[AnnexKey, str, TableRow]]:
    query = diff_query(file_key_table, filters)
    
    if limit is not None:
        query += " LIMIT %s"
        query_results = dolt.query(query, (remote_ref, local_ref, limit))
    else:
        query_results = dolt.query(query, (remote_ref, local_ref))
    for (annex_key, diff_type, *key_parts) in query_results:
        print(annex_key, diff_type, key_parts)
        yield (AnnexKey(annex_key), diff_type, TableRow(*key_parts))

def diff_query(file_key_table: FileKeyTable, filters: List[TableFilter]) -> str:
    """
    Generates a SQL query to identify the files that exist on one remote but not another.
    Note that generating a SQL query this way is not safe from SQL injection, but SQL injection
    isn't part of the threat model, since any query that the application can run,
    the user can already run themselves.
    """
    return f"""
        SELECT
            to_{file_key_table.file_column}, `diff_type`, {",".join("to_" + col for col in file_key_table.key_columns)}
        FROM dolt_commit_diff_{file_key_table.name}
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s)
        {''.join(f" AND to_{f.column_name} = %s" for f in filters)}
        """

