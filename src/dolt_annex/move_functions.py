#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import shutil

from typing_extensions import Callable

# MoveFunction is an interface for import and sync operations that attempts to move files and reports success or failure.
MoveFunction = Callable[[Path, Path], bool]

def copy(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy(src, dst)
        return True
    except (FileNotFoundError, NotADirectoryError, shutil.Error):
        return False

def move_and_symlink(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(src, dst)
        os.symlink(dst, src)
        return True
    except (FileNotFoundError, NotADirectoryError, shutil.Error):
        return False

def move(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(src, dst)
        return True
    except (FileNotFoundError, NotADirectoryError, shutil.Error):
        return False
