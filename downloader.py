#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

from typing_extensions import Dict

from config import config
from git import get_key_path
from logger import logger
from move_functions import MoveFunction
from type_hints import AnnexKey

def move_files(move: MoveFunction, files: Dict[AnnexKey, Path]):
    """Move files to the annex"""
    logger.debug("moving annex files")
    base_config = config.get()
    files_dir = Path(base_config.files_dir)
    for key, file_path in files.items():
        key_path = files_dir / get_key_path(key)
        move(file_path, key_path)
    files.clear()
