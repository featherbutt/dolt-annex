#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sys


import asyncssh
from plumbum import cli # type: ignore

from dolt_annex.application import Application
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.logger import logger
from dolt_annex.server.ssh import server_context

class Server(cli.Application):
    """Starts a sandboxed SFTP server to provide access to the filestore."""

    parent: Application

    port = cli.SwitchAttr(
        "--port",
        int,
        help="The port to listen on",
        default = 22,
    )

    host = cli.SwitchAttr(
        "--host",
        str,
        help="The host to listen on",
        default = "localhost",
    )

    authorized_keys = cli.SwitchAttr(
        "--authorized-keys",
        cli.ExistingFile,
        help="The path to an authorized public key",
        mandatory = True,
    )

    server_keyfile = cli.SwitchAttr(
        "--server-key",
        cli.ExistingFile,
        help="The path to the server key file",
        mandatory = True,
    )

    async def main(self, *args):
        """Entrypoint for server command"""
        cas = ContentAddressableStorage.from_local(self.parent.config)

        async with server_context(
            cas=cas,
            host=self.host,
            port=self.port,
            authorized_keys=self.authorized_keys,
            server_host_key=self.server_keyfile,
        ) as server:
            logger.info(f'Serving over sftp at {self.host}:{self.port}')
            await server.wait_closed()

