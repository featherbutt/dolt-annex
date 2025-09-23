#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

from typing_extensions import Dict

from git import get_key_path
from logger import logger
from move_functions import MoveFunction
from remote import Remote
from type_hints import AnnexKey

def move_files(remote: Remote, move: MoveFunction, files: Dict[AnnexKey, Path]):
    """Move files to the annex"""
    logger.debug("moving annex files")
    for key, file_path in files.items():
        key_path = remote.files_dir() / get_key_path(key)
        move(file_path, key_path)
    files.clear()
