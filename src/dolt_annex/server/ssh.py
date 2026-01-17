#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from typing_extensions import Optional

from dolt_annex.filestore.cas import ContentAddressableStorage

import asyncssh

from dolt_annex.server.sftp import SFTPServer

@asynccontextmanager
async def server_context(
    cas: ContentAddressableStorage,
    host: str,
    port: int,
    authorized_keys: Optional[str] = None,
    server_host_key: Optional[str] = None,
) -> AsyncGenerator[asyncssh.SSHServer]:

    def make_server(chan: asyncssh.SSHServerChannel):
        return SFTPServer(chan, cas)
    
    server = await asyncssh.listen(
        host=host,
        port=port,
        server_host_keys=[server_host_key],
        authorized_client_keys=authorized_keys,
        sftp_factory=make_server,
    )
    
    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()
