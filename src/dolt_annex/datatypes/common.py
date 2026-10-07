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
        default_user: str,
        default_port: int,
        default_path: str,
    ) -> Self:

        if url.startswith(url_scheme):
            url = url[len(url_scheme):]

        match url.split("@"):
            case [user, host_port_db]:
                pass
            case [host_port_db]:
                user = default_user
            case _:
                raise ValueError("Invalid format. Too many '@' symbols. Please enter in the format username@hostname:port/path.")

        match user.split(":"):
            case [user, password]:
                pass
            case [user]:
                password = False
            case _:
                raise ValueError("Invalid format. Too many ':' symbols in the user part. Please enter in the format username:password@hostname:port/path.")

        match host_port_db.split("/", maxsplit=1):
            case [host_port, path]:
                pass
            case [host_port]:
                path = default_path
            case _:
                raise ValueError("Invalid format. Unable to parse path. Please enter in the format username@hostname:port/path.")

        match host_port.split(":"):
            case [host, port_str]:
                port = int(port_str)
            case [host]:
                port = default_port
            case _:
                raise ValueError("Invalid format. Unable to parse host and port. Please enter in the format username@hostname:port/path.")

        return cls(
            hostname=host,
            port=port,
            user=user,
            password=password,
            path=Path(path)
        )


class SSHConnection(Connection):
    # password is used to decrypt the client key if client_key is not None. Otherwise it is
    # used as the SSH password.

    client_key: Path | None = None
    port: int = 22

    @classmethod
    def parse_url(
        cls,
        url: str,
        url_scheme="sftp://",
        default_user=getpass.getuser(),
        default_port=22,
        default_path=".",
    ) -> Self:
        return super().parse_url(
            url=url,
            url_scheme=url_scheme,
            default_user=default_user,
            default_port=default_port,
            default_path=default_path,
        )

class MySQLConnection(Connection):
    # path is the name of the database
    server_socket: Optional[Path] = None
    autocommit: bool = True
    port: int = 3306
    extra_params: dict[str, str] = {}

    @property
    def database(self) -> str:
        return str(self.path)

    @classmethod
    def parse_url(
        cls,
        url: str,
        url_scheme="mysql://",
        default_user="root",
        default_port=3306,
        default_path="dolt",
    ) -> Self:
        if url == "":
            raise ValueError("URL cannot be empty.")
        return super().parse_url(
            url=url,
            url_scheme=url_scheme,
            default_user=default_user,
            default_port=default_port,
            default_path=default_path,
        )