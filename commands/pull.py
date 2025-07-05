
import os
from typing_extensions import Iterable, Optional

from plumbum import cli # type: ignore

from application import Application, Downloader
from commands.push import file_mover, diff_keys, diff_keys_from_source
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
from type_hints import UUID, AnnexKey

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

    dolt_remote = cli.SwitchAttr(
        "--source",
        str,
        help="Filter pulled files to those from a specific original source",
    )

    def main(self, *args) -> int:
        """Entrypoint for pull command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            git_remote = self.git_remote or self.parent.config.git_remote
            dolt_remote = self.dolt_remote or self.parent.config.dolt_remote
            do_pull(downloader, git_remote, dolt_remote, args, self.ssh_config, None, self.limit)
        return 0

def do_pull(downloader: GitAnnexDownloader, git_remote: str, dolt_remote: str, args, ssh_config: str, known_hosts: str, source: Optional[str], limit: Optional[int] = None) -> int:
    git = downloader.git
    dolt = downloader.dolt_server
    files_pulled = 0
    local_uuid = UUID(git.config['annex.uuid'])
    remote_uuid = git.annex.get_remote_uuid(git_remote)

    dolt.pull_branch(remote_uuid, dolt_remote)
    # TODO: Fast forward if you can
    if downloader.cache.write_git_annex:
        git.fetch(git_remote, f"refs/remotes/{git_remote}/git-annex")
        git.merge_branch("refs/heads/git-annex", "refs/heads/git-annex", f"refs/remotes/{git_remote}/git-annex")

    keys: Iterable[AnnexKey]
    if len(args) == 0:
        if source is not None:
            keys = diff_keys_from_source(dolt, dolt_remote, remote_uuid, source, limit)
        else:
            keys = list(diff_keys(dolt, remote_uuid, downloader.local_uuid, limit))
    else:
        keys = args

    with file_mover(git, git_remote, ssh_config, known_hosts) as mover:
        for key in keys:
            # key_path = git.annex.get_annex_key_path(key)
            rel_key_path = git.annex.get_relative_annex_key_path(key)
            old_rel_key_path = git.annex.get_old_relative_annex_key_path(key)
            if not mover.get(rel_key_path, old_rel_key_path):
                mover.get(rel_key_path, rel_key_path)
            downloader.cache.insert_source(key, local_uuid)
            files_pulled += 1

    return files_pulled
