#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum
import getpass
from pathlib import Path
from typing import Any, Dict, Self
from typing_extensions import NewType, Optional

from dolt_annex.datatypes.pydantic import StrictBaseModel
from dolt_annex.file_keys.base import FileKey

TableRow = NewType('TableRow', Dict[str, Any])  # A row in a FileKeyTable

class YesNoMaybe(Enum):
    """
    A three-valued logic type used for the result of bloom filter lookups.
    """
    YES = "yes"
    NO = "no"
    MAYBE = "maybe"

__all__ = ['TableRow', 'YesNoMaybe', 'FileKey']

class Connection(StrictBaseModel):
    user: str = getpass.getuser()
    # password is str if provided in the config, True if prompted at runtime, False if not required
    password: str | bool = False
    hostname: str = "localhost"
    port: int
    path: Path = Path(".")

    @classmethod
    def parse_url(
        cls,
        url: str,
        url_scheme: str,
    ) -> Self:
        args = {}

        if url.startswith(url_scheme):
            url = url[len(url_scheme):]

        if url == "":
            raise ValueError("URL cannot be empty.")

        match url.split("@"):
            case [user_password, host_port_db]:
                match user_password.split(":"):
                    case [user, password]:
                        args['user'] = user
                        args['password'] = password
                    case [user]:
                        args['user'] = user
                        args['password'] = False
                    case _:
                        raise ValueError("Invalid format. Too many ':' symbols in the user part. Please enter in the format username:password@hostname:port/path.")
            case [host_port_db]:
                pass
            case _:
                raise ValueError("Invalid format. Too many '@' symbols. Please enter in the format username@hostname:port/path.")

        
        match host_port_db.split("/", maxsplit=1):
            case [host_port, path]:
                args['path'] = path
            case [host_port]:
                pass
            case _:
                raise ValueError("Invalid format. Unable to parse path. Please enter in the format username@hostname:port/path.")

        match host_port.split(":"):
            case [host, port_str]:
                args['port'] = int(port_str)
            case [host]:
                pass
            case _:
                raise ValueError("Invalid format. Unable to parse host and port. Please enter in the format username@hostname:port/path.")

        return cls(
            hostname=host,
            **args,
        )


class SSHConnection(Connection):
    # password is used to decrypt the client key if client_key is not None. Otherwise it is
    # used as the SSH password.
    user: str = getpass.getuser()
    port: int = 22
    path: Path = Path(".")

    client_key: Path | None = None

    @classmethod
    def parse_url(
        cls,
        url: str,
        url_scheme="sftp://",
    ) -> Self:
        return super().parse_url(
            url=url,
            url_scheme=url_scheme,
        )

class MySQLConnection(Connection):
    user: str = "root"
    port: int = 3306
    # path is the name of the database
    path: Path = Path("dolt")

    server_socket: Optional[Path] = None
    autocommit: bool = True
    extra_params: dict[str, str] = {}

    @property
    def database(self) -> str:
        return str(self.path)

    @classmethod
    def parse_url(
        cls,
        url: str,
        url_scheme="mysql://",
    ) -> Self:
        return super().parse_url(
            url=url,
            url_scheme=url_scheme,
        )