
import os
from typing_extensions import Iterable, Optional, Tuple

from plumbum import cli # type: ignore

from annex import SubmissionId
from application import Application, Downloader
from commands.push import FileMover, file_mover, diff_keys, diff_keys_from_source
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
    
def pull_keys(keys: Iterable[AnnexKey], git: Git, downloader: GitAnnexDownloader, mover: FileMover, local_uuid: UUID) -> int:
    files_pulled = 0
    for key in keys:
        rel_key_path = git.annex.get_relative_annex_key_path(key)
        old_rel_key_path = git.annex.get_old_relative_annex_key_path(key)
        if not mover.get(rel_key_path, old_rel_key_path):
            mover.get(rel_key_path, rel_key_path)
        downloader.cache.insert_key_source(key, local_uuid)
        files_pulled += 1
    return files_pulled

def pull_submissions_and_keys(keys_and_submissions: Iterable[Tuple[AnnexKey, SubmissionId]], git: Git, downloader: GitAnnexDownloader, mover: FileMover, local_uuid: UUID) -> int:
    files_pulled = 0
    for key, submission in keys_and_submissions:
        rel_key_path = git.annex.get_relative_annex_key_path(key)
        old_rel_key_path = git.annex.get_old_relative_annex_key_path(key)
        if not mover.get(rel_key_path, old_rel_key_path):
            mover.get(rel_key_path, rel_key_path)
        downloader.cache.insert_key_source(key, local_uuid)
        downloader.cache.insert_submission_source(submission, local_uuid)
        files_pulled += 1
    return files_pulled

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

    with file_mover(git, git_remote, ssh_config, known_hosts) as mover:
        if len(args) == 0:
            total_files_pulled = 0
            while True:
                if source is not None:
                    keys_and_submissions = diff_keys_from_source(dolt, dolt_remote, remote_uuid, source, limit)
                    files_pulled = pull_submissions_and_keys(keys_and_submissions, git, downloader, mover, local_uuid)
                else:
                    keys = list(diff_keys(dolt, remote_uuid, downloader.local_uuid, limit))
                    files_pulled = pull_keys(keys, git, downloader, mover, local_uuid)
                if files_pulled == 0:
                    break
                total_files_pulled += files_pulled
            return total_files_pulled
        else:
            return pull_keys(args, git, downloader, mover, local_uuid)

