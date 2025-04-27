#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager
import os
import socket
import threading
import time

from typing_extensions import Optional

from plumbum import cli # type: ignore
import paramiko

from application import Application
from git import Git
from logger import logger
from server import AnnexSftpServer, AnnexSshServer

class Server(cli.Application):
    """Starts a remote sandboxed SSH server"""

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
        cli.ExistingDirectory,
        help="The path to a directory containing authorized public keys",
        default = os.path.expanduser("~/.ssh"),
    )

    server_keyfile = cli.SwitchAttr(
        "--server-key",
        cli.ExistingFile,
        help="The path to the server key file",
        default = None,
    )
    
    server_key_password = cli.SwitchAttr(
        "--server-key-password",
        str,
        help="The password for the server key file",
        default = None,
    )

    def main(self, *args):
        """Entrypoint for server command"""
        base_config = self.parent.config
        git = Git(base_config.git_dir)
        if self.server_keyfile is None:
            logger.warning("No keyfile or password provided. Generating a temporary key.")
            server_key = paramiko.RSAKey.generate(bits=1024)
            logger.warning("Key fingerprint:", server_key.get_fingerprint())
        else:
            server_key = paramiko.RSAKey.from_private_key_file(self.server_keyfile, password=self.server_key_password)
        start_server(git, self.host, self.port, server_key, self.authorized_keys)

BACKLOG = 10

def run_transport(git: Git, connection: socket.socket, key: paramiko.PKey, authorized_keys_dir=None):
    ssh_server = AnnexSshServer(git.git_dir, key=key, authorized_keys_dir=authorized_keys_dir)
    transport = paramiko.Transport(connection)
    transport.add_server_key(key)
    transport.set_subsystem_handler('sftp', paramiko.SFTPServer, AnnexSftpServer, git)
    transport.start_server(server=ssh_server)
    transport.accept()


def start_server(git: Git, host, port, key, authorized_keys_dir, terminate_event=None):
    logger.debug(f'Serving over sftp at {host}:{port}')

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    server_socket.bind((host, port))
    server_socket.listen(BACKLOG)

    while terminate_event is None or not terminate_event.is_set():
        connection, _ = server_socket.accept()
        logger.debug('Established connection from %s', connection.getpeername())
        run_transport(git, connection, key, authorized_keys_dir)
        
@contextmanager
def server_context(git: Git, host: str, port: int, key: paramiko.PKey, authorized_keys_dir: Optional[str] = None):
    terminate_event = threading.Event()
    server_thread = threading.Thread(
        target=start_server,
        args=(git, host, port, key, authorized_keys_dir),
        daemon=True,
    )
    server_thread.start()
    while True:
            try:
                # Wait for the server to start
                with socket.create_connection((host, port), timeout=1) as sock:
                    logger.debug('Server started')
                    break
            except socket.error:
                logger.debug('Waiting for server to start')
                time.sleep(1)
    yield
    terminate_event.set()
    server_thread.join()
    logger.debug('Server stopped')


