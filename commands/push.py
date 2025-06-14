from contextlib import contextmanager
import os

from typing_extensions import Iterable, Optional

import sftpretty # type: ignore
from plumbum import cli # type: ignore

from application import Application, Downloader
from dolt import DoltSqlServer
from downloader import GitAnnexDownloader
from git import Git
import move_functions
from move_functions import MoveFunction
from type_hints import AnnexKey, PathLike
from logger import logger

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
        logger.info(f"Moving {abs_local_path} to {abs_remote_path}")
        self.move_function(
            abs_local_path,
            abs_remote_path)
        
    def get(self, local_path: str, remote_path: str) -> None:
        """Move a file from the local filesystem to the remote filesystem"""
        abs_local_path = PathLike(os.path.join(self.local_cwd, local_path))
        abs_remote_path = PathLike(os.path.join(self.remote_cwd, remote_path))
        logger.info(f"Moving {abs_local_path} to {abs_remote_path}")
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
def file_mover(git: Git, remote: str, ssh_config: str, known_hosts: str) -> Iterable[FileMover]:
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
            yield FileMover(sftp_put, sftp.getcwd(), local_path)            
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

    def main(self, *args) -> int:
        """Entrypoint for push command"""
        with Downloader(self.parent.config, self.batch_size) as downloader:
            git_remote = self.git_remote or self.parent.config.git_remote
            dolt_remote = self.dolt_remote or self.parent.config.dolt_remote
            do_push(downloader, git_remote, dolt_remote, args, self.ssh_config, None, self.limit)
        return 0

def do_push(downloader: GitAnnexDownloader, git_remote: str, dolt_remote: str, args, ssh_config: str, known_hosts: str, limit: Optional[int] = None) -> int:
    git = downloader.git
    dolt = downloader.dolt_server
    files_pushed = 0
    remote_uuid = git.annex.get_remote_uuid(git_remote)

    dolt.pull_branch(remote_uuid, dolt_remote)
    # TODO: Fast forward if you can
    git.fetch(git_remote, "git-annex")
    git.merge_branch("refs/heads/git-annex", "refs/heads/git-annex", f"refs/remotes/{git_remote}/git-annex")

    keys: Iterable[AnnexKey]
    if len(args) == 0:
        keys = list(diff_keys(dolt, downloader.local_uuid, remote_uuid, limit))
    else:
        keys = args

    with file_mover(git, git_remote, ssh_config, known_hosts) as mover:
        for key in keys:
            # key_path = git.annex.get_annex_key_path(key)
            rel_key_path = git.annex.get_relative_annex_key_path(key)
            try:
                mover.put(rel_key_path, rel_key_path)
            except Exception:
                rel_key_path = git.annex.get_old_relative_annex_key_path(key)
                mover.put(rel_key_path, rel_key_path)
            downloader.cache.insert_source(key, remote_uuid)
            files_pushed += 1

    downloader.flush()

    # with dolt.set_branch(remote_uuid):
    #    dolt.commit(False, amend=True)
    
    # Push the git branch
    git.push_branch(git_remote, "git-annex")
    # Push the dolt branch
    dolt.push_branch("main", dolt_remote)
    dolt.push_branch(git.annex.uuid, dolt_remote)
    dolt.push_branch(remote_uuid, dolt_remote)
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
        if limit is not None:
            query = dolt.query("SELECT diff_type, `to_annex-key` FROM dolt_commit_diff_local_keys WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s) LIMIT %s;", (not_in_ref, union_branch_name, limit))
        else:
            query = dolt.query("SELECT diff_type, `to_annex-key` FROM dolt_commit_diff_local_keys WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s);", (not_in_ref, union_branch_name))
        for (diff_type, annex_key) in query:
            assert diff_type == "added"
            yield AnnexKey(annex_key)
