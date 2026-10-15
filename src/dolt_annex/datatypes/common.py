#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum
from typing import Any, Dict
from typing_extensions import NewType

TableRow = NewType('TableRow', Dict[str, Any])  # A row in a FileKeyTable
