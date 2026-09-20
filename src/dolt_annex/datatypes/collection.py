#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID
import pathlib

from .loader import Loadable
    
class Collection(Loadable, extension="collection", config_dir=pathlib.Path("collections")):
    """
    A description of a file respository. May be local or remote.
    """

    type Id = UUID

    uuid: Id
