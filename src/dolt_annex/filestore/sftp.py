#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The use of SFTP as a protocol is largely historical: since originally the only supported filestore
was AnnexFS, a client could use SFTP to connect to a remote server and access its AnnexFS filestore
directly. This is still an option if the client already has SSH access to the remote server, and
the protocol is designed to have identical observed behavior in either case.

A single SFTP connection can only transfer one file at a time.
"""

# TODO: Add support for parallel connections to increase throughput.

from contextlib import contextmanager
import getpass
import hashlib
from pathlib import Path
from paramiko import SFTPFile
from typing_extensions import Optional, ContextManager, override, Generator, BinaryIO

import sftpretty

from dolt_annex.datatypes.config import Config
from dolt_annex.file_keys import FileKey, FileKeyType

from .base import FileStore, copy

class SftpFileStore(FileStore):

    file_key_format: FileKeyType
    host: str
    port: int
    username: str
    path: Path

    _sftp: sftpretty.Connection

    @override
    def put_file_object(self, in_fd: BinaryIO, file_key: Optional[FileKey] = None) -> None:
        """Upload a file-like object to the remote. If file_key is not provided, it will be computed."""
        if file_key is None:
            file_key = self.file_key_format.from_fo(in_fd)
        remote_file_path = self.get_key_path(file_key).as_posix()
        self._sftp.mkdir_p(Path(remote_file_path).parent.as_posix())
        out_fd: SFTPFile
        with self._sftp.open(remote_file_path, mode='wb') as out_fd:
            copy(src=in_fd, dst=out_fd)

    @override
    @contextmanager
    def get_file_object(self, file_key: FileKey) -> Generator[BinaryIO]:
        """Get a file-like object for a file in the remote by its key."""
        remote_file_path = self.get_key_path(file_key).as_posix()
        if not self._sftp.exists(remote_file_path):
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        with self._sftp.open(remote_file_path, mode='rb') as f:
            yield f

    @override
    def open(self, config: Config) -> ContextManager[None]:
        """Connect to an SFTP filestore."""

        @contextmanager
        def inner() -> Generator[None]:
            cnopts = sftpretty.CnOpts(config=config.ssh.ssh_config, knownhosts=config.ssh.known_hosts)
            cnopts.log_level = 'error'

            extra_opts = {}
            if config.ssh.encrypted_ssh_key:
                extra_opts["private_key_pass"] = getpass.getpass("Enter passphrase for private key: ")

            with sftpretty.Connection(self.host, port=self.port, cnopts=cnopts, username=self.username, default_path=self.path.as_posix(), **extra_opts) as self._sftp:
                yield
            
        return inner()
    
    @override
    def flush(self):
        pass

    def get_key_path(self, key: FileKey) -> Path:
        """
        Get the relative path for a file in the annex from its key.

        This is copied from AnnexFS, allowing clients to connect to AnnexFS filestores over SFTP.

        When connecting to a Dolt-Annex server, the path is ignored.
        """
        md5 = hashlib.md5(bytes(key)).hexdigest()
        return Path('.') / md5[:3] / md5[3:6] / str(key)

    @override
    def exists(self, file_key: FileKey) -> bool:
        return self._sftp.exists(self.get_key_path(file_key).as_posix())
