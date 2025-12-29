#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum
import getpass
from pathlib import Path
from typing_extensions import NewType, Optional

from dolt_annex.datatypes.pydantic import StrictBaseModel
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

class SSHConnection(StrictBaseModel):
    user: str = getpass.getuser()
    hostname: str = "localhost"
    port: int = 22
    client_key: Path | None = None
    path: Path = Path(".")

class MySQLConnection(StrictBaseModel):
    hostname: str = "localhost"
    port: int = 3306
    server_socket: Optional[Path] = None
    user: str = "root"
    password: str | None = None
    database: str = "dolt"
    autocommit: bool = True
    extra_params: dict[str, str] = {}