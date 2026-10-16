#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID
import pathlib

from .loader import Loadable
    
class Collection(Loadable, extension="collection", config_dir=pathlib.Path("collections")):
    """
    A set of submission records, such as a playlist or a favorite list.
    """

    type Id = UUID

    uuid: Id
