#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utility type declarations"""

import uuid

from typing_extensions import NewType, TYPE_CHECKING

PathLike = NewType('PathLike', str)
AnnexKey = NewType('AnnexKey', str)

UUID = NewType('UUID', str)
if not TYPE_CHECKING:
    def new_uuid(val: str) -> str:
        assert is_valid_uuid(val)
        return val
    UUID = new_uuid

def is_valid_uuid(val: str):
    try:
        return str(uuid.UUID(val)) == val
    except ValueError:
        return False
