import os

from plumbum import local

from dry_run import dry_run
from logger import logger

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
        self.git_dir = git_dir
        self.dry_run = dry_run
    
    # TODO: Replace these commands with continuous batch commands
    def calckey(self, key: str):
        return self.cmd("calckey", key).strip()
    
    def get_branch_key_path(self, key: bytes):
        return self.cmd("examinekey", "--format=${hashdirlower}${key}", str(key, encoding="utf8")).strip()
        
    def get_annex_key_path(self, key: str):
        rel_path = self.cmd("examinekey", "--format=${hashdirlower}${key}/${key}", key).strip()
        return os.path.abspath(os.path.join(self.git_dir, "annex", "objects", rel_path))
    
    @dry_run("Would register {key} with url {url}")
    def registerurl(self, key: str, url: str):
        return self.cmd("registerurl", key, url)
    
    @dry_run("Would add {file_path} to annex with key {key}")
    def setkey(self, key: str, file_path: str):
        return self.cmd("setkey", key, file_path)
    
    @dry_run("Would transfer key {key} to remote {remote}")
    def push_content(self, key: str, remote: str = "origin"):
        return self.cmd("transferkey", key, "--to", remote)
 

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


    

    

