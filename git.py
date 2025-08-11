from dataclasses import dataclass
import hashlib
import os
import pathlib

from config import config

from type_hints import AnnexKey, PathLike

def key_from_file(key_path: PathLike) -> AnnexKey:
    path = pathlib.Path(key_path)
    extension = path.suffix[1:]  # Get the file extension without the dot
    with open(key_path, 'rb') as f:
        data = f.read()
    data_hash = hashlib.sha256(data).hexdigest()
    return AnnexKey(f"SHA256E-s{len(data)}--{data_hash}.{extension}")

def get_branch_key_path(key: AnnexKey) -> PathLike:
    # return self.cmd("examinekey", "--format=${hashdirlower}${key}", key).strip()
    md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
    return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}")
        
def get_relative_annex_key_path(key: AnnexKey) -> PathLike:
    md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
    return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}")

def get_annex_key_path(key: AnnexKey) -> PathLike:
    md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
    return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}")

def get_old_relative_annex_key_path(key: AnnexKey) -> PathLike:
    md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
    return PathLike(f"{md5[:3]}/{md5[3:6]}/{key}/{key}")

def get_absolute_file_path(path: PathLike) -> PathLike:
    if not os.path.isabs(path):
        return PathLike(os.path.abspath(os.path.join(config.get().files_dir, path)))

    return path

@dataclass
class ConnectionInfo:
    user: str
    host: str
    path: str
