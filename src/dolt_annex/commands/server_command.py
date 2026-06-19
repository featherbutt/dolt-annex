#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

from plumbum import cli # type: ignore

from dolt_annex.application import Application
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.server.ssh import server_context

logger = logging.getLogger(__name__)
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
        help="The path to an authorized keys file, which specifies the public keys that are allowed to connect to the server",
        mandatory = True,
    )

    server_keyfile = cli.SwitchAttr(
        "--server-key",
        cli.ExistingFile,
        help="The path to the server key file, used to authenticate the server to clients",
        mandatory = True,
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The name of the repo to serve. If not specified, serves the default repo.",
    )

    async def main(self, *args):
        """Entrypoint for server command"""
        config: Config = self.parent.config

        async with Repo.open(config, self.repo) as repo:
            async with (
                server_context(
                    cas=repo.filestore,
                    host=self.host,
                    port=self.port,
                    authorized_keys=self.authorized_keys,
                    server_host_key=self.server_keyfile,
                ) as server,
            ):
                logger.info("Serving over sftp at %s:%d", self.host, self.port)
                await server.wait_closed()

