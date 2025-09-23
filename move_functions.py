#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import shutil

from typing_extensions import Callable

MoveFunction = Callable[[Path, Path], bool]

def copy(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy(src, dst)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False

def move_and_symlink(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(src, dst)
        os.symlink(dst, src)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False

def move(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(src, dst)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False
