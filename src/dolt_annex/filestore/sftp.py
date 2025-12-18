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

from contextlib import asynccontextmanager
import hashlib
from pathlib import Path
from typing import AsyncGenerator, cast
import asyncssh
from typing_extensions import override

from dolt_annex.datatypes.config import Config, resolve_path
from dolt_annex.datatypes.common import SSHConnection
from dolt_annex.datatypes.file_io import FileObject, ReadableFileObject
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, copy

class SftpFileStore(FileStore):

    connection: SSHConnection

    _sftp: asyncssh.SFTPClient = None

    @override
    async def put_file_object(self, in_fd: ReadableFileObject, file_key: FileKey) -> None:
        """Upload a file-like object to the remote."""
        remote_file_path = self.get_key_path(file_key).as_posix()
        # self._sftp.mkdir_p(Path(remote_file_path).parent.as_posix())
        await self._sftp.makedirs(Path(remote_file_path).parent.as_posix(), exist_ok=True)
        async with self._sftp.open(remote_file_path, 'wb') as out_fd:
            await copy(src=in_fd, dst=out_fd)

    @override
    async def get_file_object(self, file_key: FileKey) -> FileObject:
        """Get a file-like object for a file in the remote by its key."""
        remote_file_path = self.get_key_path(file_key).as_posix()
        
        if not await self.exists(file_key):
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        return await self._sftp.open(remote_file_path, 'rb').__aenter__()

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
         file_obj = await self.get_file_object(file_key)
         return await self.fstat(file_obj)

    @override
    async def fstat(self, file_obj: ReadableFileObject) -> FileInfo:
        sftp_file_obj = cast(asyncssh.SFTPClientFile, file_obj)
        stat_result = await sftp_file_obj.stat()
        return FileInfo(size=stat_result.size)


    @override
    @asynccontextmanager
    async def open(self, config: Config) -> AsyncGenerator[None, None]:
        """Connect to an SFTP filestore."""

        if self._sftp is None:
            extra_opts = {}
            if config.ssh.encrypted_ssh_key:
                # TODO: Support passphrase input for encrypted SSH keys
                pass
            
            client_keys = []
            if self.connection.client_key is not None:
                client_keys.append(self.connection.client_key)

            async with asyncssh.connect(
                host=self.connection.hostname,
                port=self.connection.port,
                known_hosts=None,
                subsystem="sftp",
                config=resolve_path(config.ssh.ssh_config),
                client_keys=client_keys,
                **extra_opts
            ) as conn:
                async with conn.start_sftp_client() as self._sftp:
                    yield
        else:
            yield
    
    @override
    async def flush(self):
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
    async def exists(self, file_key: FileKey) -> bool:
        try:
            # We don't call SFTPClient.exists because it checks for the type attribute,
            # which does not exist prior to SFTP protocol version 4.
            return bool(await self._sftp.stat(self.get_key_path(file_key).as_posix()))
        except asyncssh.SFTPNoSuchFile:
            return False