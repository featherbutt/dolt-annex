#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utility type declarations"""

from typing_extensions import NewType

AnnexKey = NewType('AnnexKey', str)
TableRow = NewType('TableRow', tuple)  # A row in a FileKeyTable