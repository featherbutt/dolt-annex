#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum
from typing_extensions import NewType

from dolt_annex.file_keys.base import FileKey as AnnexKey

TableRow = NewType('TableRow', tuple)  # A row in a FileKeyTable

class YesNoMaybe(Enum):
    """
    A three-valued logic type used for the result of bloom filter lookups.
    """
    YES = "yes"
    NO = "no"
    MAYBE = "maybe"

__all__ = ['TableRow', 'YesNoMaybe', 'AnnexKey']
