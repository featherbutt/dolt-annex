from contextlib import contextmanager
import os

from typing_extensions import Iterable, Optional, Generator, Tuple

import sftpretty # type: ignore
from plumbum import cli # type: ignore

from annex import SubmissionId
from application import Application, Downloader
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import move_functions
from move_functions import MoveFunction
from type_hints import UUID, AnnexKey, PathLike
from logger import logger

class FileMover:
    local_cwd: str
    remote_cwd: str
    move_function: MoveFunction

    def __init__(self, put_function: MoveFunction, get_function: MoveFunction, remote_cwd: str, local_cwd = None) -> None:
        if local_cwd is None:
            local_cwd = os.getcwd()
        self.local_cwd = os.path.abspath(local_cwd)
        self.remote_cwd = os.path.abspath(remote_cwd)
        self.put_function = put_function
        self.get_function = get_function

    def put(self, local_path: str, remote_path: str) -> bool:
        """Move a file from the local filesystem to the remote filesystem"""
        abs_local_path = PathLike(os.path.join(self.local_cwd, local_path))
        abs_remote_path = PathLike(os.path.join(self.remote_cwd, remote_path))
        logger.info(f"Moving {abs_local_path} to {abs_remote_path}")
        return self.put_function(
            abs_local_path,
            abs_remote_path)
        
    def get(self, local_path: str, remote_path: str) -> bool:
        """Move a file from the local filesystem to the remote filesystem"""
        abs_local_path = PathLike(os.path.join(self.local_cwd, local_path))
        abs_remote_path = PathLike(os.path.join(self.remote_cwd, remote_path))
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
            self.local_cwd = os.path.abspath(os.path.join(self.local_cwd, local_path))
        if remote_path is not None:
            self.remote_cwd = os.path.abspath(os.path.join(self.remote_cwd, remote_path))
        yield
        self.local_cwd = old_local_cwd
        self.remote_cwd = old_remote_cwd

@contextmanager
def file_mover(git: Git, remote: str, ssh_config: str, known_hosts: str) -> Generator[FileMover, None, None]:
    remote_path = git.get_remote_url(remote)
    local_path = os.path.join(os.getcwd(), git.git_dir)
    if '@' in remote_path:
        if os.path.exists(os.path.join(local_path, '.git')):
            local_path = os.path.join(local_path, '.git')
        local_path = os.path.join(local_path, 'annex/objects')
        user, rest = remote_path.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        cnopts = sftpretty.CnOpts(config = ssh_config, knownhosts = known_hosts)
        cnopts.log_level = 'error'
        with sftpretty.Connection(host, cnopts=cnopts, username = user, default_path = path) as sftp:
            if sftp.exists('.git'):
                sftp.chdir('.git')
            sftp.mkdir_p('annex/objects')
            sftp.chdir('annex/objects')
            def sftp_put(
                local_path: PathLike,
                remote_path: PathLike,
            ) -> None:
                """Move a file from the local filesystem to the remote filesystem using SFTP"""
                sftp.mkdir_p(os.path.dirname(remote_path))
                if sftp.exists(remote_path):
                    logger.info(f"File {remote_path} already exists, skipping")
                    return
                sftp.put(local_path, remote_path)
            def sftp_get(
                remote_path: PathLike,
                local_path: PathLike,
            ) -> bool:
                """Move a file from the remote filesystem to the local filesystem using SFTP"""
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                if not sftp.exists(remote_path):
                    return False
                if os.path.exists(local_path):
                    logger.info(f"File {local_path} already exists, skipping")
                    return False
                sftp.get(remote_path, local_path)
                return True
            yield FileMover(sftp_put, sftp_get, sftp.getcwd(), local_path)
    else:
        # Remote path may be relative to the local git directory
        remote_path = os.path.join(local_path, remote_path)
        if os.path.exists(os.path.join(remote_path, '.git')):
            remote_path = os.path.join(remote_path, '.git')
        remote_path = os.path.join(remote_path, 'annex/objects')
        if os.path.exists(os.path.join(local_path, '.git')):
            local_path = os.path.join(local_path, '.git')
        local_path = os.path.join(local_path, 'annex/objects')
        yield FileMover(move_functions.copy, move_functions.copy, remote_path, local_path)

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
        help="The maximum number of files to push",
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

    source = cli.SwitchAttr(
        "--source",
        str,
        help="Filter pushed files to those from a specific original source",
    )

    def main(self, *args) -> int:
        """Entrypoint for push command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            git_remote = self.git_remote or self.parent.config.git_remote
            dolt_remote = self.dolt_remote or self.parent.config.dolt_remote
            do_push(downloader, git_remote, dolt_remote, args, self.ssh_config, self.known_hosts, self.source, self.limit)
        return 0

def do_push(downloader: GitAnnexDownloader, git_remote: str, dolt_remote: str, args, ssh_config: str, known_hosts: str, source: Optional[str], limit: Optional[int] = None) -> int:
    git = downloader.git
    dolt = downloader.dolt_server
    files_pushed = 0
    remote_uuid = git.annex.get_remote_uuid(git_remote)
    local_uuid = downloader.local_uuid

    dolt.pull_branch(remote_uuid, dolt_remote)
    # TODO: Fast forward if you can
    if downloader.cache.write_git_annex:
        git.fetch(git_remote, "git-annex")
        git.merge_branch("refs/heads/git-annex", "refs/heads/git-annex", f"refs/remotes/{git_remote}/git-annex")

    with file_mover(git, git_remote, ssh_config, known_hosts) as mover:
        if len(args) == 0:
            total_files_pushed = 0
            while True:
                if source is not None:
                    keys_and_submissions = diff_keys_from_source(dolt, local_uuid, remote_uuid, source, limit)
                    files_pushed = push_submissions_and_keys(keys_and_submissions, git, downloader, mover, remote_uuid)
                else:
                    keys = list(diff_keys(dolt, local_uuid, remote_uuid, limit))
                    files_pushed = push_keys(keys, git, downloader, mover, local_uuid)
                if files_pushed == 0:
                    break
                total_files_pushed += files_pushed
            return total_files_pushed
        else:
            return push_keys(args, git, downloader, mover, local_uuid)
    downloader.flush()

    # with dolt.set_branch(remote_uuid):
    #    dolt.commit(False, amend=True)

    # Push the git branch
    if downloader.cache.write_git_annex and downloader.cache.auto_push:
        git.push_branch(git_remote, "git-annex")
    # Push the dolt branch
    if downloader.cache.auto_push:
        dolt.push_branch("main", dolt_remote)
        dolt.push_branch(git.annex.uuid, dolt_remote)
        dolt.push_branch(remote_uuid, dolt_remote)
    return files_pushed

def push_keys(keys: Iterable[AnnexKey], git: Git, downloader: GitAnnexDownloader, mover: FileMover, remote_uuid: UUID) -> int:
    files_pushed = 0
    for key in keys:
        rel_key_path = git.annex.get_relative_annex_key_path(key)
        old_rel_key_path = git.annex.get_old_relative_annex_key_path(key)
        if not mover.put(old_rel_key_path, rel_key_path):
            mover.put(rel_key_path, rel_key_path)
        downloader.cache.insert_key_source(key, remote_uuid)
        files_pushed += 1
    return files_pushed

def push_submissions_and_keys(keys_and_submissions: Iterable[Tuple[AnnexKey, SubmissionId]], git: Git, downloader: GitAnnexDownloader, mover: FileMover, remote_uuid: UUID) -> int:
    files_pushed = 0
    for key, submission in keys_and_submissions:
        rel_key_path = git.annex.get_relative_annex_key_path(key)
        old_rel_key_path = git.annex.get_old_relative_annex_key_path(key)
        if not mover.put(old_rel_key_path, rel_key_path):
            mover.put(rel_key_path, rel_key_path)
        downloader.cache.insert_submission_source(submission, remote_uuid)
        files_pushed += 1
    return files_pushed

def pull_personal_branch(git: Git, dolt: DoltSqlServer, remote: str) -> None:
    """Fetch the personal branch for the remote"""
    remote_uuid = git.annex.get_remote_uuid(remote)
    dolt.pull_branch(remote_uuid, remote)

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, limit = None) -> Iterable[AnnexKey]:
    """
    Return each key that is in the first ref but not in the second ref.

    This is more complicated than just selecting from dolt_commit_diff_local_keys, because
    a simple diff returns all changes, including keys that are in the second ref but not the first.
    If multiple clients are pushing to the same server, most of these keys won't be keys the client can send.

    Instead, we create a third branch containing the union of both branch's keys, then compute which keys
    are in the union but not in the second branch. This union branch only needs to be created once in order
    to push or pull annexed files, because the union doesn't change as files are copied to/from the server.
    """
    refs = [in_ref, not_in_ref]
    refs.sort()
    union_branch_name = f"union-{refs[0]}-{refs[1]}"
    # Create the union branch if it doesn't exist
    
    with dolt.maybe_create_branch(union_branch_name, in_ref):
        dolt.merge(in_ref)
        dolt.merge(not_in_ref)
        query = """
        SELECT `to_annex-key`
        FROM dolt_commit_diff_local_keys
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s) AND diff_type = 'added'
        """
        if limit is not None:
            query += " LIMIT %s"
            query_results = dolt.query(query, (not_in_ref, union_branch_name, limit))
        else:
            query_results = dolt.query(query, (not_in_ref, union_branch_name))
        for (annex_key,) in query_results:
            yield AnnexKey(annex_key)

def diff_keys_from_source(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, source: str, limit = None) -> Iterable[Tuple[AnnexKey, SubmissionId]]:
    refs = [in_ref, not_in_ref]
    refs.sort()
    union_branch_name = f"union-{refs[0]}-{refs[1]}"
    # Create the union branch if it doesn't exist
    
    with dolt.maybe_create_branch(union_branch_name, in_ref):
        dolt.merge(in_ref)
        dolt.merge(not_in_ref)
        query = """
        SELECT
            `annex-key`, `to_source`, `to_id`, `to_updated`, `to_part`
        FROM dolt_commit_diff_local_submissions
        JOIN filenames AS OF submissions
            ON source = to_source AND id = to_id AND updated = to_updated AND part = to_part
        JOIN `annex-keys` AS OF files
            ON `annex-keys`.url = filenames.url
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s) AND to_source = %s AND diff_type = 'added'
        """
        if limit is not None:
            query += " LIMIT %s"
            query_results = dolt.query(query, (not_in_ref, union_branch_name, source, limit))
        else:
            query_results = dolt.query(query, (not_in_ref, union_branch_name, source))
        for (annex_key, to_source, to_sid, to_updated, to_part) in query_results:
            assert to_source == source
            yield (AnnexKey(annex_key), SubmissionId(to_source, to_sid, to_updated, to_part))