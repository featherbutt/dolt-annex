from dataclasses import dataclass
import hashlib
import os
from pathlib import Path

from typing_extensions import Optional

import config

from type_hints import AnnexKey

def key_from_file(key_path: Path, extension: Optional[str] = None) -> AnnexKey:
    if extension is None:
        extension = key_path.suffix[1:]  # Get the file extension without the dot
    with open(key_path, 'rb') as f:
        data = f.read()
    data_hash = hashlib.sha256(data).hexdigest()
    return AnnexKey(f"SHA256E-s{len(data)}--{data_hash}.{extension}")

def get_key_path(key: AnnexKey) -> Path:
    md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
    return Path(f"{md5[:3]}/{md5[3:6]}/{key}")
        
def get_old_relative_annex_key_path(key: AnnexKey) -> Path:
    md5 = hashlib.md5(key.encode('utf-8')).hexdigest()
    return Path(f"{md5[:3]}/{md5[3:6]}/{key}/{key}")

def get_absolute_file_path(path: Path) -> Path:
    if not path.is_absolute():
        return (Path(config.get_config().files_dir) / path).resolve()

    return path

@dataclass
class ConnectionInfo:
    user: str
    host: str
    path: str
