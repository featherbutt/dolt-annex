#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
from io import BufferedReader, BufferedWriter
import os
from pathlib import Path
import tempfile

from paramiko import SFTPServerInterface, SFTPServer, SFTPAttributes, \
    ServerInterface
import paramiko

from git import get_key_path
from type_hints import AnnexKey

CHUNK_SIZE = 8092

class AnnexSftpServer (SFTPServerInterface):
    """A custom SFTP server for adding and retrieving files from the annex.
    
    Because files in the annex are content-addressed, the user doesn't need to provie the full path.
    Instead, they can just provide the key. The server will then compute the full path to the file.
    This potentially allows for remotes to choose their own storage backend without affecting the client.
    This also allows the server to validate that the file contents match the key.
    
    The user is allowed to supply a path, but it is ignored. mkdir operations are also ignored.
    This allows the same client to work against both regular SFTP remotes and this server.

    Operations other than put, get, stat, and mkdir are deliberately not supported,
    in order to prevent clients from overwriting data.
    """

    def __init__(self, server: ServerInterface):
        SFTPServerInterface.__init__(self, server)

    def stat(self, path):
        path = real_path(path)
        try:
            return SFTPAttributes.from_stat(os.stat(path))
        except OSError as e:
            return SFTPServer.convert_errno(e.errno)

    def lstat(self, path):
        path = real_path(path)
        try:
            return SFTPAttributes.from_stat(os.lstat(path))
        except OSError as e:
            return SFTPServer.convert_errno(e.errno)

    def open(self, path, flags, attr) -> paramiko.SFTPHandle | int:
        # Supported operations are limited to read and create
        key = key_from_path(path)
        try:
            if flags & os.O_CREAT:
                annex_file_location = get_key_path(key)
                if annex_file_location.exists():
                    raise FileExistsError(f"File {key} already exists, and overwriting existing files is not supported")
                return NewFileHandle(flags, key)
            else:
                return ExistingFileHandle(flags, key)
        except OSError as e:
            return SFTPServer.convert_errno(e.errno) # type: ignore
        

class ExistingFileHandle(paramiko.SFTPHandle):
    """A file handle for reading an existing key"""

    # SFTPHandle checks for this attribute and uses it for IO
    readfile: BufferedReader

    def __init__(self, flags, key: AnnexKey):
        super().__init__(flags)
        annex_file_location = real_path_from_key(key)
        self.readfile = open(annex_file_location, "rb")
    
    def stat(self):
        try:
            return SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
        except OSError as e:
            return SFTPServer.convert_errno(e.errno)
        
class NewFileHandle(paramiko.SFTPHandle):
    """A file handle for uploading a new key.
    
    On creation, the file is created in a temporary location.
    After the file is closed, the correct path is computed and the file is moved to the
    final location. This both prevents partial writes and also allows for the file contents
    to be verified before moving it into the annex."""

    # SFTPHandle checks for this attribute and uses it for IO
    writefile: BufferedWriter

    key: AnnexKey
    suffix: str

    def __init__(self, flags, key: AnnexKey):
        super().__init__(flags)
        self.key = key
        self.suffix = Path(key).suffix
        self.writefile = tempfile.NamedTemporaryFile(delete=False, suffix=self.suffix, buffering=CHUNK_SIZE) # type: ignore
        
    def close(self):
        # Compute the file key, based on its length, extension, and SHA256 hash
        file_length = self.writefile.tell()
        self.writefile.seek(0)
        hasher = hashlib.new("sha256")
        while chunk := self.writefile.read(CHUNK_SIZE):
            hasher.update(chunk)
        sha256_hash = hasher.hexdigest()

        actual_key = f"SHA256E-s{file_length}--{sha256_hash}{self.suffix}"
        if actual_key != self.key:
            raise ValueError(f"Supplied key {self.key} does not match the computed key {actual_key}")
        
        # Close the file handle
        super().close()

        # Move the file to the annex location
        annex_file_location = get_key_path(self.key)
        annex_file_location.parent.mkdir(parents=True, exist_ok=True)
        os.rename(self.writefile.name, annex_file_location)

def key_from_path(path: Path) -> AnnexKey:
    """Extract the key from a user-supplied path"""
    return AnnexKey(path.name)

def real_path_from_key(key: AnnexKey) -> Path:
    """Compute the filesystem path for a key"""
    return get_key_path(key)

def real_path(path: Path) -> Path:
    """Given a user-supplied path to a key, return the real path where the corresponding file is stored"""
    return real_path_from_key(key_from_path(path))

