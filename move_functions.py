#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pathlib
import shutil

from typing_extensions import Callable

from type_hints import PathLike


MoveFunction = Callable[[PathLike, PathLike], bool]

def copy(src: str, dst: str):
    pathlib.Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy(src, dst)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False

def move_and_symlink(src: str, dst: str):
    pathlib.Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(src, dst)
        os.symlink(dst, src)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False

def move(src: str, dst: str):
    print("moving from", src, "to", dst)
    pathlib.Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(src, dst)
        return True
    except (FileNotFoundError, NotADirectoryError):
        return False
