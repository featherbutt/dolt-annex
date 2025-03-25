#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pathlib
import shutil
from typing import Callable

from type_hints import PathLike


MoveFunction = Callable[[PathLike, PathLike], None]

def copy(src: str, dst: str):
    pathlib.Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
    return shutil.copy(src, dst)

def move_and_symlink(src: str, dst: str):
    pathlib.Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
    shutil.move(src, dst)
    os.symlink(dst, src)

def move(src: str, dst: str):
    pathlib.Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
    return shutil.move(src, dst)
