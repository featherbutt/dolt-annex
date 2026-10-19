#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

from plumbum import cli

from dolt_annex.commands import SubCommand
from dolt_annex.config.base import Config
from dolt_annex.datatypes.repo import Repo
from dolt_annex.server.ssh import server_context

logger = logging.getLogger(__name__)
class Server(SubCommand):
    """Starts a sandboxed SFTP server to provide access to the filestore."""

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
    )

    password = cli.SwitchAttr(
        "--password",
        str,
        help="The password for the server, used to authenticate clients",
        excludes=["--authorized-keys"],
    )

    no_authentication = cli.Flag(
        "--no-authentication",
        help="Allow any client to connect without authenticating",
        excludes=["--authorized-keys", "--password"],
    )

    server_keyfile = cli.SwitchAttr(
        "--server-key",
        cli.ExistingFile,
        help="The path to the server key file, used to authenticate the server to clients",
        mandatory = True,
    )

    server_keyfile_passphrase = cli.SwitchAttr(
        "--server-key-passphrase",
        str,
        help="The passphrase for the server key file, if it is encrypted",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The name of the repo to serve. If not specified, serves the default repo.",
    )

    async def main(self):
        """Entrypoint for server command"""
        auth_options = [self.authorized_keys is not None, self.password is not None, self.no_authentication]
        if sum(auth_options) != 1:
            print("Exactly one of --authorized-keys, --password, or --no-authentication must be specified.")
            return 1

        config: Config = self.config

        async with Repo.open(config, self.repo) as repo:
            async with (
                server_context(
                    cas=repo,
                    host=self.host,
                    port=self.port,
                    authorized_keys=self.authorized_keys,
                    server_host_key=self.server_keyfile,
                    server_keyfile_passphrase=self.server_keyfile_passphrase,
                    password=self.password,
                ) as server,
            ):
                logger.info("Serving over sftp at %s:%d", self.host, self.port)
                await server.wait_closed()

