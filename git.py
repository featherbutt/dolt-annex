from dataclasses import dataclass
import hashlib
import json
import os

from typing_extensions import Iterable

from plumbum import local # type: ignore

from dry_run import dry_run
from type_hints import UUID, AnnexKey, PathLike

class GitConfig:
    def __init__(self, git_cmd):
        self.cmd = git_cmd["config", "--local"]

    def __getitem__(self, key: str) -> str:
        return self.cmd(key).strip()

    def get(self, key: str, default = None) -> str:
        return self.cmd("--default", str(default), key).strip()

    def __setitem__(self, key: str, value: str):
        self.cmd(key, value)

    def __delitem__(self, key: str):
        self.cmd('--unset', key)

class GitAnnex:
    def __init__(self, git_cmd, git_dir: str):
        self.cmd = git_cmd["annex"]
        self.git_cmd = git_cmd
        self.git_dir = git_dir
        self.dry_run = dry_run
        self.uuid = git_cmd("config", "annex.uuid").strip()

    # TODO: Replace these commands with continuous batch commands
    def calckey(self, key_path: PathLike) -> AnnexKey:
        return self.cmd("calckey", key_path).strip()

    def get_branch_key_path(self, key: AnnexKey) -> PathLike:
        # return self.cmd("examinekey", "--format=${hashdirlower}${key}", key).strip()
        md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
        return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}")
           
    def get_relative_annex_key_path(self, key: AnnexKey) -> PathLike:
        md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
        return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}")

    def get_annex_key_path(self, key: AnnexKey) -> PathLike:
        md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
        rel_path = f"{md5[:3]}/{md5[3:6]}/{key}"
        return PathLike(os.path.abspath(os.path.join(self.git_dir, "annex", "objects", rel_path)))

    def get_old_relative_annex_key_path(self, key: AnnexKey) -> PathLike:
        md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
        return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}/{key}")

    def get_old_annex_key_path(self, key: AnnexKey) -> PathLike:
        md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
        rel_path = f"{md5[:3]}/{md5[3:6]}/{key}/{key}"
        return PathLike(os.path.abspath(os.path.join(self.git_dir, "annex", "objects", rel_path)))

    @dry_run("Would register {key} with url {url}")
    def registerurl(self, key: str, url: str):
        return self.cmd("registerurl", key, url)
        
    def is_present(self, key: str) -> bool:
        returncode, _, _ = self.cmd.run(["readpresentkey", key, self.uuid], retcode=None)
        return returncode == 0

    @dry_run("Would add {file_path} to annex with key {key}")
    def setkey(self, key: str, file_path: str):
        return self.cmd("setkey", key, file_path)

    @dry_run("Would transfer key {key} to remote {remote}")
    def push_content(self, key: str, remote: str = "origin"):
        return self.cmd("transferkey", key, "--to", remote)
    
    def get_remote_uuid(self, remote: str) -> UUID:
        """Get the uuid of a remote"""
        remote_info = json.loads(self.cmd("info", remote, "--json", "--fast"))
        return remote_info["uuid"]
    
    def sync(self):
        self.cmd("sync", "--no-content")

@dataclass
class ConnectionInfo:
    user: str
    host: str
    path: str
class Git:
    def __init__(self, git_dir: str):
        self.git_dir = git_dir
        self.cmd = local.cmd.git["-C", git_dir]
        self.config = GitConfig(self.cmd)
        self.annex = GitAnnex(self.cmd, git_dir)

    def show(self, ref: str, path: str) -> str:
        return self.cmd('show', f'{ref}:{path}', retcode=None)

    def popen(self, *args, **kwargs):
        return self.cmd.popen(*args, **kwargs)
    
    def get_remote_url(self, remote: str) -> str:
        return self.cmd("remote", "get-url", remote).strip()

    def get_remote_info(self, remote: str) -> ConnectionInfo:
        remote_url = self.get_remote_url(remote)
        # could be a file or an ssh url
        user, rest = remote_url.split('@', maxsplit=1)
        host, path = rest.split(':', maxsplit=1)
        return ConnectionInfo(user, host, path)
    
    def merge_branch(self, into_branch: str, *from_refs: Iterable[str]):
        from_oids = [self.cmd("show-ref", ref, "-s").strip() for ref in from_refs]
        new_tree = self.cmd("merge-tree", "--no-messages", *from_oids).strip()
        new_commit = self.cmd("commit-tree", new_tree, *[x for oid  in from_oids for x in ["-p", oid]], "-m", "Merge").strip()
        self.cmd("update-ref", into_branch, new_commit)

    def push_branch(self, remote: str, branch: str):
        return self.cmd("push", remote, branch).strip()

    def get_revision(self, ref: str) -> str:
        return self.cmd("rev-parse", ref).strip()
    
    def fetch(self, remote: str, ref: str) -> str:
        return self.cmd("fetch", remote, ref).strip()