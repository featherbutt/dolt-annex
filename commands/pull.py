from contextlib import contextmanager
import os

from typing_extensions import Iterable, Optional

import sftpretty # type: ignore
from plumbum import cli # type: ignore

from application import Application, Downloader
from commands.push import file_mover
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import move_functions
from move_functions import MoveFunction
from type_hints import UUID, AnnexKey, PathLike
from logger import logger

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
        default = "~/.ssh/known_hosts",
    )

    limit = cli.SwitchAttr(
        "--limit",
        int,
        help="The maximum number of files to pull",
        default = None,
    )

    git_remote = cli.SwitchAttr(
        "--git-remote",
        str,
        help="The name of the git remote",
    )

    dolt_remote = cli.SwitchAttr(
        "--dolt-remote",
        str,
        help="The name of the dolt remote",
    )

    def main(self, *args) -> int:
        """Entrypoint for pull command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            git_remote = self.git_remote or self.parent.config.git_remote
            dolt_remote = self.dolt_remote or self.parent.config.dolt_remote
            do_pull(downloader, git_remote, dolt_remote, args, self.ssh_config, None, self.limit)
        return 0

def do_pull(downloader: GitAnnexDownloader, git_remote: str, dolt_remote: str, args, ssh_config: str, known_hosts: Optional[str], limit: Optional[int] = None) -> int:
    git = downloader.git
    dolt = downloader.dolt_server
    files_pulled = 0
    local_uuid = UUID(git.config['annex.uuid'])
    remote_uuid = git.annex.get_remote_uuid(git_remote)

    dolt.pull_branch(local_uuid, dolt_remote)
    # TODO: Fast forward if you can
    git.fetch(git_remote, f"refs/remotes/{git_remote}/git-annex")
    git.merge_branch("refs/heads/git-annex", "refs/heads/git-annex", f"refs/remotes/{git_remote}/git-annex")

    keys: Iterable[AnnexKey]
    if len(args) == 0:
        keys = list(diff_keys(dolt, remote_uuid, downloader.local_uuid, limit))
    else:
        keys = args

    with file_mover(git, git_remote, ssh_config, known_hosts) as mover:
        for key in keys:
            # key_path = git.annex.get_annex_key_path(key)
            rel_key_path = git.annex.get_relative_annex_key_path(key)
            try:
                mover.get(rel_key_path, rel_key_path)
            except Exception:
                old_rel_key_path = git.annex.get_old_relative_annex_key_path(key)
                mover.get(old_rel_key_path, rel_key_path)
            downloader.cache.insert_source(key, remote_uuid)
            files_pulled += 1

    return files_pulled

def pull_personal_branch(git: Git, dolt: DoltSqlServer, remote: str) -> None:
    """Fetch the personal branch for the remote"""
    remote_uuid = git.annex.get_remote_uuid(remote)
    dolt.pull_branch(remote_uuid, remote)

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, limit = None) -> Iterable[AnnexKey]:
    """Return each key that is in the first ref but not in the second ref"""
    with dolt.set_branch(in_ref):
        if limit is not None:
            query = dolt.query("SELECT diff_type, `to_annex-key` FROM dolt_commit_diff_local_keys WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s) LIMIT %s;", (not_in_ref, in_ref, limit))
        else:
            query = dolt.query("SELECT diff_type, `to_annex-key` FROM dolt_commit_diff_local_keys WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s);", (not_in_ref, in_ref))
        for (diff_type, annex_key) in query:
            if diff_type == "added":
                yield AnnexKey(annex_key)
