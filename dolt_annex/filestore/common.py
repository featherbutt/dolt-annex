#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
from pathlib import Path

from dolt_annex import config
from dolt_annex.datatypes import AnnexKey

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
    