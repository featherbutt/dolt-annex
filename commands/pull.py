#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID

from typing_extensions import Iterable, Optional, Tuple

from plumbum import cli # type: ignore

from annex import SubmissionId
from config import get_config
from application import Application, Downloader
from commands.push import FileMover, file_mover, diff_keys, diff_keys_from_source
from downloader import GitAnnexDownloader
from git import get_old_relative_annex_key_path, get_key_path
from logger import logger
from remote import Remote
from type_hints import AnnexKey

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

    source = cli.SwitchAttr(
        "--source",
        str,
        help="Filter pulled files to those from a specific original source",
    )

    def main(self, *args) -> int:
        """Entrypoint for pull command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            remote_name = self.remote or self.parent.config.dolt_remote
            remote = Remote.from_name(remote_name)
            if not remote:
                logger.error(f"Remote {remote_name} not found")
                return 1
            do_pull(downloader, remote, args, self.ssh_config, self.known_hosts, self.source, self.limit)
        return 0
    
def pull_keys(keys: Iterable[AnnexKey], downloader: GitAnnexDownloader, mover: FileMover, local_uuid: UUID) -> int:
    files_pulled = 0
    for key in keys:
        rel_key_path = get_key_path(key)
        old_rel_key_path = get_old_relative_annex_key_path(key)
        if not mover.get(rel_key_path, old_rel_key_path):
            mover.get(rel_key_path, rel_key_path)
        downloader.cache.insert_key_source(key, local_uuid)
        files_pulled += 1
    return files_pulled

def pull_submissions_and_keys(keys_and_submissions: Iterable[Tuple[AnnexKey, SubmissionId]], downloader: GitAnnexDownloader, mover: FileMover, local_uuid: UUID) -> int:
    files_pulled = 0
    for key, submission in keys_and_submissions:
        rel_key_path = get_key_path(key)
        old_rel_key_path = get_old_relative_annex_key_path(key)
        if not mover.get(rel_key_path, old_rel_key_path):
            mover.get(rel_key_path, rel_key_path)
        downloader.cache.insert_key_source(key, local_uuid)
        downloader.cache.insert_submission_source(submission, local_uuid)
        files_pulled += 1
    return files_pulled

def do_pull(downloader: GitAnnexDownloader, remote: Remote, args, ssh_config: str, known_hosts: str, source: Optional[str], limit: Optional[int] = None) -> int:
    dolt = downloader.dolt_server
    files_pulled = 0
    local_uuid = get_config().local_uuid
    remote_uuid = remote.uuid

    dolt.pull_branch(str(remote_uuid), remote)

    with file_mover(remote, ssh_config, known_hosts) as mover:
        if len(args) == 0:
            total_files_pulled = 0
            while True:
                if source is not None:
                    keys_and_submissions = diff_keys_from_source(dolt, str(local_uuid), str(remote_uuid), source, limit)
                    files_pulled = pull_submissions_and_keys(keys_and_submissions, downloader, mover, local_uuid)
                else:
                    keys = list(diff_keys(dolt, str(remote_uuid), str(local_uuid), limit))
                    files_pulled = pull_submissions_and_keys(keys, downloader, mover, local_uuid)
                if files_pulled == 0:
                    break
                total_files_pulled += files_pulled
            return total_files_pulled
        else:
            return pull_keys(args, downloader, mover, local_uuid)

