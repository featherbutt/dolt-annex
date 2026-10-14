#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hmac
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from typing_extensions import Optional

from dolt_annex.filestore.cas import ContentAddressableStorage

import asyncssh

from dolt_annex.server.sftp import SFTPServer

class _ClientAuthentication(asyncssh.SSHServer):
    """
    Authenticates clients by password.
    """

    def __init__(self, password: Optional[str]):
        self._password = password

    def begin_auth(self, username: str) -> bool:
        return self._password is not None

    def password_auth_supported(self) -> bool:
        return self._password is not None

    def validate_password(self, username: str, password: str) -> bool:
        return self._password is not None and hmac.compare_digest(password, self._password)

@asynccontextmanager
async def server_context(
    cas: ContentAddressableStorage,
    host: str,
    port: int,
    authorized_keys: Optional[str] = None,
    server_host_key: Optional[str] = None,
    server_keyfile_passphrase: Optional[str] = None,
    password: Optional[str] = None,
) -> AsyncGenerator[asyncssh.SSHAcceptor]:

    def make_server(chan: asyncssh.SSHServerChannel):
        return SFTPServer(chan, cas)

    server = await asyncssh.listen(
        host=host,
        port=port,
        server_host_keys=[server_host_key],
        server_factory=lambda: _ClientAuthentication(password=password),
        authorized_client_keys=authorized_keys,
        password_auth=password is not None,
        passphrase=server_keyfile_passphrase,
        sftp_factory=make_server,
    )

    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()
