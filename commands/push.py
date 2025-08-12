from contextlib import contextmanager
import os
from typing import List

from typing_extensions import Iterable, Optional, Generator, Tuple

import sftpretty # type: ignore
from plumbum import cli # type: ignore

import context
from annex import SubmissionId
from application import Application, Downloader
from config import get_config
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import get_old_relative_annex_key_path, get_relative_annex_key_path
import move_functions
from move_functions import MoveFunction
from remote import Remote
from type_hints import UUID, AnnexKey, PathLike
from logger import logger

class FileMover:
    local_cwd: str
    remote_cwd: str
    put_function: MoveFunction
    get_function: MoveFunction

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
def file_mover(remote: Remote, ssh_config: str, known_hosts: Optional[str]) -> Generator[FileMover, None, None]:
    base_config = get_config()
    local_path = os.path.abspath(base_config.files_dir)
    if '@' in remote.url:
        user, rest = remote.url.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        cnopts = sftpretty.CnOpts(config = ssh_config, knownhosts = known_hosts)
        cnopts.log_level = 'error'
        with sftpretty.Connection(host, cnopts=cnopts, username = user, default_path = path) as sftp:
            def sftp_put(
                local_path: PathLike,
                remote_path: PathLike,
            ) -> bool:
                """Move a file from the local filesystem to the remote filesystem using SFTP"""
                if not os.path.exists(local_path):
                    return False
                sftp.mkdir_p(os.path.dirname(remote_path))
                if sftp.exists(remote_path):
                    logger.info(f"File {remote_path} already exists, skipping")
                    return True
                sftp.put(local_path, remote_path)
                return True
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
                    return True
                sftp.get(remote_path, local_path)
                return True
            yield FileMover(sftp_put, sftp_get, sftp.getcwd(), local_path)
    elif remote.url.startswith("file://"):
        # Remote path may be relative to the local git directory
        yield FileMover(move_functions.copy, move_functions.copy, remote.url[7:], local_path)
    else:
        raise ValueError(f"Unknown remote URL format: {remote.url}")
    
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

    remote = cli.SwitchAttr(
        "--remote",
        str,
        help="The name of the dolt-annex remote",
    )

    source = cli.SwitchAttr(
        "--source",
        str,
        help="Filter pushed files to those from a specific original source",
    )

    def main(self, *args) -> int:
        """Entrypoint for push command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            remote_name = self.remote or self.parent.config.dolt_remote
            remote = Remote.from_name(remote_name)
            do_push(downloader, remote, args, self.ssh_config, self.known_hosts, self.source, self.limit)
        return 0

def do_push(downloader: GitAnnexDownloader, file_remote: Remote, args, ssh_config: str, known_hosts: Optional[str], source: Optional[str], limit: Optional[int] = None) -> List[AnnexKey]:
    dolt = downloader.dolt_server
    remote_uuid = file_remote.uuid
    local_uuid = get_config().local_uuid

    # TODO: Dolt remote not necessarily the same as file remote, know when pull is necessary
    # dolt.pull_branch(remote_uuid, dolt_remote)

    with file_mover(file_remote, ssh_config, known_hosts) as mover:
        if len(args) == 0:
            total_files_pushed = []
            while True:
                if source is not None:
                    keys_and_submissions = diff_keys_from_source(dolt, local_uuid, remote_uuid, source, limit)
                    files_pushed = push_submissions_and_keys(keys_and_submissions, downloader, mover, remote_uuid)
                else:
                    keys_and_submissions = list(diff_keys(dolt, local_uuid, remote_uuid, limit))
                    files_pushed = push_submissions_and_keys(keys_and_submissions, downloader, mover, remote_uuid)
                if len(files_pushed) == 0:
                    break
                total_files_pushed += files_pushed
        else:
            total_files_pushed = push_keys(args, downloader, mover, local_uuid)
    downloader.flush()

    return total_files_pushed

def push_keys(keys: Iterable[AnnexKey], downloader: GitAnnexDownloader, mover: FileMover, remote_uuid: UUID) -> List[AnnexKey]:
    files_pushed = []
    for key in keys:
        rel_key_path = get_relative_annex_key_path(key)
        old_rel_key_path = get_old_relative_annex_key_path(key)
        if not mover.put(old_rel_key_path, rel_key_path):
            mover.put(rel_key_path, rel_key_path)
        downloader.cache.insert_key_source(key, remote_uuid)
        files_pushed.append(key)
    downloader.flush()
    return files_pushed

def push_submissions_and_keys(keys_and_submissions: Iterable[Tuple[AnnexKey, SubmissionId]], downloader: GitAnnexDownloader, mover: FileMover, remote_uuid: UUID) -> List[AnnexKey]:
    files_pushed = []
    for key, submission in keys_and_submissions:
        rel_key_path = get_relative_annex_key_path(key)
        old_rel_key_path = get_old_relative_annex_key_path(key)
        if not mover.put(old_rel_key_path, rel_key_path):
            mover.put(rel_key_path, rel_key_path)
        downloader.cache.insert_submission_source(submission, remote_uuid)
        files_pushed.append(key)
    downloader.flush()
    return files_pushed

def pull_personal_branch(dolt: DoltSqlServer, remote: Remote) -> None:
    """Fetch the personal branch for the remote"""
    dolt.pull_branch(remote.uuid, remote)

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, limit = None) -> Iterable[Tuple[AnnexKey, SubmissionId]]:
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
        SELECT
            `file_key`, `to_source`, `to_id`, `to_updated`, `to_part`
        FROM dolt_commit_diff_local_submissions
        JOIN file_keys AS OF files
            ON source = to_source AND id = to_id AND updated = to_updated AND part = to_part
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s) AND diff_type = 'added'
        """
        if limit is not None:
            query += " LIMIT %s"
            query_results = dolt.query(query, (not_in_ref, union_branch_name, limit))
        else:
            query_results = dolt.query(query, (not_in_ref, union_branch_name))
        for (annex_key, to_source, to_sid, to_updated, to_part) in query_results:
            yield (AnnexKey(annex_key), SubmissionId(to_source, to_sid, to_updated, to_part))

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
            `file_key`, `to_source`, `to_id`, `to_updated`, `to_part`
        FROM dolt_commit_diff_local_submissions
        JOIN file_keys AS OF files
            ON source = to_source AND id = to_id AND updated = to_updated AND part = to_part
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