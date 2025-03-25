from contextlib import contextmanager
import os
from typing import Iterable, Optional

import sftpretty # type: ignore
from plumbum import cli # type: ignore

from application import Application, Downloader
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import move_functions
from move_functions import MoveFunction
from type_hints import AnnexKey, PathLike

class FileMover:
    local_cwd: str
    remote_cwd: str
    move_function: MoveFunction

    def __init__(self, move_function: MoveFunction, remote_cwd: str, local_cwd = None) -> None:
        if local_cwd is None:
            local_cwd = os.getcwd()
        self.local_cwd = os.path.abspath(local_cwd)
        self.remote_cwd = os.path.abspath(remote_cwd)
        self.move_function = move_function

    def put(self, local_path: str, remote_path: str) -> None:
        """Move a file from the local filesystem to the remote filesystem"""
        abs_local_path = PathLike(os.path.join(self.local_cwd, local_path))
        abs_remote_path = PathLike(os.path.join(self.remote_cwd, remote_path))
        self.move_function(
            abs_local_path,
            abs_remote_path)

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
def file_mover(git: Git, remote: str):
    remote_path = git.get_remote_url(remote)
    local_path = os.path.join(os.getcwd(), git.git_dir)
    if '@' in remote_path:
        if os.path.exists(os.path.join(local_path, '.git')):
            local_path = os.path.join(local_path, '.git')
        local_path = os.path.join(local_path, 'annex/objects')
        user, rest = remote_path.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        with sftpretty.Connection(host, username = user, default_path = path) as sftp:
            if sftp.exists('.git'):
                sftp.chdir('.git')
            sftp.chdir('annex/objects')
            yield FileMover(sftp.put, sftp.getcwd(), local_path)            
    else:
        # Remote path may be relative to the local git directory
        remote_path = os.path.join(local_path, remote_path)
        if os.path.exists(os.path.join(remote_path, '.git')):
            remote_path = os.path.join(remote_path, '.git')
        remote_path = os.path.join(remote_path, 'annex/objects')
        if os.path.exists(os.path.join(local_path, '.git')):
            local_path = os.path.join(local_path, '.git')
        local_path = os.path.join(local_path, 'annex/objects')
        yield FileMover(move_functions.copy, remote_path, local_path)

class Push(cli.Application):
    """Push imported files to a remote repository"""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
        default = 1000,
    )

    def main(self, remote, *args) -> int:
        """Entrypoint for push command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            do_push(downloader, remote, args)
        return 0

def do_push(downloader: GitAnnexDownloader, remote: str, args) -> int:
    git = downloader.git
    dolt = downloader.dolt_server
    remote_uuid = git.annex.get_remote_uuid(remote)
    files_pushed = 0

    dolt.pull_branch(remote_uuid, remote)
    # TODO: Fast forward if you can
    git.merge_branch("refs/heads/git-annex", "refs/heads/git-annex", f"refs/remotes/{remote}/git-annex")

    keys: Iterable[AnnexKey]
    if len(args) == 0:
        keys = diff_keys(dolt, downloader.local_uuid, remote_uuid)
    else:
        keys = args

    with file_mover(git, remote) as mover:
        for key in keys:
            # key_path = git.annex.get_annex_key_path(key)
            rel_key_path = git.annex.get_relative_annex_key_path(key)
            mover.put(rel_key_path, rel_key_path)
            downloader.cache.insert_source(key, remote_uuid)
            files_pushed += 1

    return files_pushed

def pull_personal_branch(git: Git, dolt: DoltSqlServer, remote: str) -> None:
    """Fetch the personal branch for the remote"""
    remote_uuid = git.annex.get_remote_uuid(remote)
    dolt.pull_branch(remote_uuid, remote)

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str) -> Iterable[AnnexKey]:
    """Return each key that is in the first ref but not in the second ref"""
    with dolt.set_branch(in_ref):
        for (diff_type, annex_key) in dolt.execute("SELECT diff_type, `to_annex-key` FROM dolt_commit_diff_local_keys WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s);", (not_in_ref, in_ref)):
            if diff_type == "added":
                yield AnnexKey(annex_key)
