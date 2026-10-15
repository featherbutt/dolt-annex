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
import hashlib
import logging
from pathlib import Path
import random
from typing import Optional, Self
import asyncssh
from typing_extensions import AsyncGenerator, override

from asyncssh.sftp import SFTPError
from dolt_annex.datatypes.async_utils import await_or_enter
from dolt_annex.config.base import Config, resolve_path
from dolt_annex.config.connection import SSHConnection
from dolt_annex.datatypes.async_types import ReadableFileObject, ReadableStream
from dolt_annex.file_keys import FileKey
from dolt_annex.datatypes.file_stream import KeyedFileStream
from dolt_annex.util.prompts import secret

from .base import FileInfo, FileStore, FileStoreError, FileStoreModel, copy, wrap_errors

logger = logging.getLogger(__name__)

@dataclass(kw_only=True)
class SftpFileStore(FileStore):

    sftp: asyncssh.SFTPClient

    @override
    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def put_file_object(self, data_source: KeyedFileStream, overwrite_existing: bool = False) -> FileStore.PutFileResult:
        """Upload a file-like object to the remote."""
        temp_remote_file_path = f"tmp/{random.randbytes(16).hex()}"

        await self.sftp.makedirs(Path(temp_remote_file_path).parent.as_posix(), exist_ok=True)
        async with (
            data_source as in_fd,
            self.sftp.open(temp_remote_file_path, 'wb') as out_fd,
        ):
            await copy(src=in_fd, dst=out_fd)
        file_key = await data_source.file_key()
        real_remote_file_path = self.get_key_path(file_key).as_posix()
        await self.sftp.makedirs(Path(real_remote_file_path).parent.as_posix(), exist_ok=True)

        await self.sftp.posix_rename(temp_remote_file_path, real_remote_file_path)
        return FileStore.PutFileResult(file_key=file_key, uuids=[self.uuid] if self.uuid else [])

    @override
    @await_or_enter
    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def with_file_object(self, file_key: FileKey) -> AsyncGenerator[ReadableFileObject]:
        """Get a file-like object for a file in the remote by its key."""
        remote_file_path = self.get_key_path(file_key).as_posix()
        if await self.sftp.isdir(remote_file_path):
            remote_file_path = self.get_old_key_path(file_key).as_posix()

        if not await self.exists(file_key):
            raise FileNotFoundError(f"File with key {file_key} not found in annex.")
        
        async with await self.sftp.open(remote_file_path, 'rb') as file_obj:
            yield file_obj

    @override
    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def stat(self, file_key: FileKey) -> FileInfo:
        async with self.with_file_object(file_key) as file_obj:
             return await self.fstat(file_obj)

    @override
    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def fstat(self, file_obj: ReadableStream) -> FileInfo:
        if not isinstance(file_obj, asyncssh.SFTPClientFile):
            raise TypeError("SftpFileStore.fstat was passed a file object that did not originate from this filestore.")
        stat_result = await file_obj.stat()
        return FileInfo(size=stat_result.size)
    
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
    
    def get_old_key_path(self, key: FileKey) -> Path:
        """
        Get the relative path for an annex key using the old layout that includes an extra directory
        with the same name as the key.

        Some older versions of dolt-annex used this layout, so we fall back to it when looking for files.
        """
        md5 = hashlib.md5(bytes(key)).hexdigest()
        return Path('.') / md5[:3] / md5[3:6] / str(key) / str(key)


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
    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def create_alias(self, old_key: FileKey, new_key: FileKey) -> None:
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

    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def get_remote_uuid(self) -> Optional[FileStore.Id]:
        try:
            async with self.sftp.open("uuid", "rb") as f:
                return FileStore.Id(bytes=await f.read())
        except asyncssh.SFTPNoSuchFile:
            return None

    @classmethod
    @asynccontextmanager
    @wrap_errors(wrap=SFTPError, into=FileStoreError)
    async def open(cls, filestore_id: Optional[FileStore.Id], connection: SSHConnection, config: Config) -> AsyncGenerator[Self]:
        """Connect to an SFTP filestore."""
        extra_opts = {}
        match connection.password:
            case str(password_value):
                if connection.client_key:
                    extra_opts["passphrase"] = password_value
                else:
                    extra_opts["password"] = password_value
            case True:
                if connection.client_key:
                    passphrase = await secret("Enter passphrase for SSH key: ")
                    extra_opts["passphrase"] = passphrase
                else:
                    password = await secret("Enter password for SSH connection: ")
                    extra_opts["password"] = password
        
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
            username=connection.user,
            **extra_opts
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                await sftp.chdir(connection.path.as_posix())
                filestore = cls(uuid=filestore_id, sftp=sftp)
                if filestore.uuid is not None:
                    remote_uuid = await filestore.get_remote_uuid()
                    if remote_uuid and remote_uuid != filestore.uuid:
                        raise FileStoreError("Local UUID does not match remote UUID")
                yield filestore
        
class SftpFileStoreModel(FileStoreModel):
    
    connection: SSHConnection

    @override
    def open(self, config: Config) -> _AsyncGeneratorContextManager[SftpFileStore]:
        """Connect to an SFTP filestore."""
        return SftpFileStore.open(self.uuid, self.connection, config)