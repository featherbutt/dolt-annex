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

from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from dataclasses import dataclass
import getpass
import hashlib
from pathlib import Path
from typing import Self
import asyncssh
from typing_extensions import AsyncGenerator, override

from dolt_annex.datatypes.async_utils import Result, await_or_enter
from dolt_annex.datatypes.config import Config, resolve_path
from dolt_annex.datatypes.common import SSHConnection
from dolt_annex.datatypes.async_types import AsyncContextManager, ReadableFileObject, ReadableStream
from dolt_annex.file_keys import FileKey

from .base import FileInfo, FileStore, FileStoreModel, copy

@dataclass
class SftpFileStore(FileStore):

    sftp: asyncssh.SFTPClient

    @override
    async def put_file_object(self, data_source: AsyncContextManager[ReadableStream], file_key: FileKey) -> Result[None]:
        """Upload a file-like object to the remote."""
        remote_file_path = self.get_key_path(file_key).as_posix()
        await self.sftp.makedirs(Path(remote_file_path).parent.as_posix(), exist_ok=True)
        async with (
            self.sftp.open(remote_file_path, 'wb') as out_fd,
            data_source as in_fd,
        ):
            await copy(src=in_fd, dst=out_fd)
        return Result.of(None)

    @override
    @await_or_enter
    async def get_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""
        remote_file_path = self.get_key_path(file_key).as_posix()
        
        if not await self.exists(file_key):
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        yield await self.sftp.open(remote_file_path, 'rb')

    @override
    async def stat(self, file_key: FileKey) -> FileInfo:
         file_obj = await self.get_file_object(file_key)
         return await self.fstat(file_obj)

    @override
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, asyncssh.SFTPClientFile):
            raise TypeError("SftpFileStore.fstat was passed a file object that did not originate from this filestore.")
        stat_result = await file_obj.stat()
        return FileInfo(size=stat_result.size)
    
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
    async def exists(self, file_key: FileKey) -> bool:
        try:
            # We don't call SFTPClient.exists because it checks for the type attribute,
            # which does not exist prior to SFTP protocol version 4.
            stat = await self.sftp.stat(self.get_key_path(file_key).as_posix())
            return bool(stat)
        except asyncssh.SFTPNoSuchFile:
            return False
  
    @override
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> Result[None]:
        new_relative_path = self.get_key_path(new_key).as_posix()
        await self.sftp.makedirs(Path(new_relative_path).parent.as_posix(), exist_ok=True)

        # If we supply a relative path for `old_path`, it will get
        # interpreted relative to the server's CWD. We need to make it absolute.
        old_absolute_path = self.sftp.compose_path(self.get_key_path(old_key).as_posix())
        new_absolute_path = self.sftp.compose_path(new_relative_path)

        await self.sftp.symlink(
            oldpath=old_absolute_path,
            newpath=new_absolute_path,
        )
        return Result.of(None)

    @classmethod
    @asynccontextmanager
    async def open(cls, connection: SSHConnection, config: Config) -> AsyncGenerator[Self]:
        """Connect to an SFTP filestore."""
        extra_opts = {}
        if connection.key_is_encrypted:
            passphrase = getpass.getpass("Enter passphrase for SSH key: ")
            extra_opts["passphrase"] = passphrase
        
        client_keys = []
        if connection.client_key is not None:
            client_keys.append(connection.client_key)

        async with asyncssh.connect(
            host=connection.hostname,
            port=connection.port,
            known_hosts=None,
            subsystem="sftp",
            config=resolve_path(config.ssh.ssh_config),
            client_keys=client_keys,
            **extra_opts
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.chdir(connection.path.as_posix())
                filestore = cls(sftp)
                yield filestore
        
class SftpFileStoreModel(FileStoreModel):
    
    connection: SSHConnection

    @override
    def open(self, config: Config) -> _AsyncGeneratorContextManager[SftpFileStore]:
        """Connect to an SFTP filestore."""
        return SftpFileStore.open(self.connection, config)