from plumbum import local

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
    def __init__(self, git_cmd):
        self.cmd = git_cmd["annex"]
    
    def calckey(self, key: str):
        return self.cmd("calckey", key).strip()
    
    def registerurl(self, key: str, url: str):
        return self.cmd("registerurl", key, url)
    
    def setkey(self, key: str, file_path: str):
        return self.cmd("setkey", key, file_path)
    
    def push_content(self, key: str, remote: str = "origin"):
        return self.cmd("transferkey", key, "--to", remote)
 
    
class Git:
    def __init__(self, git_dir: str):
        self.git_dir = git_dir
        self.cmd = local.cmd.git["-C", git_dir]
        self.config = GitConfig(self.cmd)
        self.annex = GitAnnex(self.cmd)
    
    def show(self, ref: str, path: str) -> str:
        return self.cmd('show', f'{ref}:{path}', retcode=None)
    
    def popen(self, *args, **kwargs):
        return self.cmd.popen(*args, **kwargs)


    

    

