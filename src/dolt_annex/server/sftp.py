#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BufferedWriter
import os
import pathlib
import tempfile
from types import TracebackType
from typing_extensions import Any, Buffer, Optional, override

import asyncssh
from asyncssh.misc import MaybeAwait
import fs.osfs
from fs.base import FS as FileSystem

from dolt_annex.file_keys.base import FileKey
from dolt_annex.logger import logger
from dolt_annex.datatypes import AnnexKey
from dolt_annex.datatypes.file_io import Path
from dolt_annex.filestore.base import FileInfo, FileObject, ReadableFileObject, maybe_await
from dolt_annex.filestore.cas import ContentAddressableStorage

class SFTPServer(asyncssh.SFTPServer):

    temp_dir: tempfile.TemporaryDirectory
    temp_file_system: FileSystem

    def __init__(self, chan: asyncssh.SSHServerChannel, cas: ContentAddressableStorage):
        self.cas = cas
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_file_system = fs.osfs.OSFS(self.temp_dir.name)
        super().__init__(chan, chroot=os.path.realpath(self.temp_dir.name).encode('utf-8'))

    def exit(self) -> None:
        self.temp_file_system.close()
        self.temp_dir.cleanup()
        super().exit()

    def format_user(self, uid: Optional[int]) -> str:
        """Return the user name associated with a uid

           :param uid:
               The uid value to look up
           :type uid: `int` or `None`

           :returns: The formatted user name string

        """
        raise asyncssh.SFTPOpUnsupported("user info is not supported")

    def format_group(self, gid: Optional[int]) -> str:
        """Return the group name associated with a gid

           :param gid:
               The gid value to look up
           :type gid: `int` or `None`

           :returns: The formatted group name string

        """

        raise asyncssh.SFTPOpUnsupported("group info is not supported")

    def format_longname(self, name: asyncssh.SFTPName) -> MaybeAwait[None]:
        """Format the long name associated with an SFTP name

           :param name:
               The :class:`SFTPName` instance to format the long name for
           :type name: :class:`SFTPName`

        """
        raise asyncssh.SFTPOpUnsupported("long name formatting is not supported")

    @override
    async def open(self, path: bytes, pflags: int, attrs: asyncssh.SFTPAttrs) -> ReadableFileObject:
        logger.info(f"Opening file: {path}")

        if not (pflags & (asyncssh.FXF_READ | asyncssh.FXF_CREAT)):
            raise asyncssh.SFTPOpUnsupported("Only read and create operations are supported")
        
        # Supported operations are limited to read and create
        key = self.cas.file_key_format(path.rsplit(b'/')[-1])
        if pflags & asyncssh.FXF_CREAT:
            return await self.create_file(key)
        else:
            # If create flag is not set, read must be set.
            return await self.open_file_for_read(key)
        

    @override
    async def open56(self, path: bytes, desired_access: int, flags: int,
               attrs: asyncssh.SFTPAttrs) -> ReadableFileObject:
        """Open a file to serve to a remote client (SFTPv5 and later)

           :param path:
               The name of the file to open
           :param desired_access:
               The access mode to use for the file (see above)
           :param flags:
               The access flags to use for the file (see above)
           :param attrs:
               File attributes to use if the file needs to be created
           :type path: `bytes`
           :type desired_access: `int`
           :type flags: `int`
           :type attrs: :class:`SFTPAttrs`

           :returns: A file object to use to access the file

           :raises: :exc:`SFTPError` to return an error to the client

        """
        logger.info(f"Opening file: {path}")

        if not (flags & (asyncssh.FXF_OPEN_EXISTING | asyncssh.FXF_CREATE_NEW)):
            raise asyncssh.SFTPOpUnsupported("Only read and create operations are supported")
        
        # Supported operations are limited to read and create
        key = self.cas.file_key_format(path.rsplit(b'/')[-1])
        if flags & asyncssh.FXF_CREATE_NEW:
            return await self.create_file(key)
        else:
            # If create flag is not set, read must be set.
            return await self.open_file_for_read(key)
        
    async def create_file(self, key: FileKey) -> FileObject:
        if await maybe_await(self.cas.file_store.exists(key)):
            raise asyncssh.SFTPOpUnsupported(f"File {key} already exists, and overwriting existing files is not supported")

        return NewFileHandle(self.temp_file_system, self.cas, key)
    
    async def open_file_for_read(self, key: FileKey) -> ReadableFileObject:
        return await maybe_await(self.cas.file_store.get_file_object(key))
        
    @override
    async def close(self, file_obj: Any) -> None:
        if not isinstance(file_obj, NewFileHandle):
            await maybe_await(file_obj.close())
            return
        
        file_obj.writefile.seek(0)

        actual_key = self.cas.file_key_format.from_fo(file_obj.writefile, file_obj.suffix)
        if actual_key != file_obj.key:
            raise ValueError(f"Supplied key {file_obj.key} does not match the computed key {actual_key}")
        
        # Close the file handle
        file_obj.close()

        # Move the file to the annex location
        # When calling, indicate whether file is being moved, deleted, or neither.
        await maybe_await(self.cas.file_store.put_file(Path(self.temp_file_system, pathlib.Path(file_obj.writefile.name).name), file_key=file_obj.key))
        # Delete the temporary file unless put_file moved it.
        if os.path.exists(file_obj.writefile.name):
            os.remove(file_obj.writefile.name)

    @override
    async def stat(self, path: bytes) -> asyncssh.SFTPAttrs:
        """Get attributes of a file or directory, following symlinks

           This method queries the attributes of a file or directory.
           If the path provided is a symbolic link, the returned
           attributes should correspond to the target of the link.

           :param path:
               The path of the remote file or directory to get attributes for
           :type path: `bytes`

           :returns: An :class:`SFTPAttrs` or an os.stat_result containing
                     the file attributes

           :raises: :exc:`SFTPError` to return an error to the client

        """
        if key := self.cas.file_key_format.try_parse(path.rsplit(b'/')[-1]):
            # If the last part is a valid key, assume it's a file
            file_info = await maybe_await(self.cas.file_store.stat(key))
            if file_info:
                return asyncssh.SFTPAttrs(
                    type=asyncssh.FILEXFER_TYPE_REGULAR,
                    size=file_info.size,
                )
            raise asyncssh.SFTPNoSuchFile(f"Key {key} does not exist on this filestore.")
           
        # Otherwise assume it's a directory
        return asyncssh.SFTPAttrs(asyncssh.FILEXFER_TYPE_DIRECTORY)
    
    @override
    async def fstat(self, file_obj: Any) -> asyncssh.SFTPAttrs:
        """Get attributes of an open file

           :param file_obj:
               The file to get attributes for
           :type file_obj: file

           :returns: An :class:`SFTPAttrs` or an os.stat_result containing
                     the file attributes

           :raises: :exc:`SFTPError` to return an error to the client

        """
        if isinstance(file_obj, NewFileHandle):
            file_info = file_obj.file_info()
        else:
            file_info = await maybe_await(self.cas.file_store.fstat(file_obj))
        return asyncssh.SFTPAttrs(
            type=asyncssh.FILEXFER_TYPE_REGULAR,
            size=file_info.size,
        )

    def lstat(self, path: bytes) -> MaybeAwait[asyncssh.SFTPAttrs]:
        """Get attributes of a file, directory, or symlink

           This method queries the attributes of a file, directory,
           or symlink. Unlike :meth:`stat`, this method should
           return the attributes of a symlink itself rather than
           the target of that link.

           :param path:
               The path of the file, directory, or link to get attributes for
           :type path: `bytes`

           :returns: An :class:`SFTPAttrs` or an os.stat_result containing
                     the file attributes

           :raises: :exc:`SFTPError` to return an error to the client

        """
        return self.stat(path)

    def setstat(self, path: bytes, attrs: asyncssh.SFTPAttrs) -> MaybeAwait[None]:
        """Set attributes of a file or directory

           This method sets attributes of a file or directory. If
           the path provided is a symbolic link, the attributes
           should be set on the target of the link. A subset of the
           fields in `attrs` can be initialized and only those
           attributes should be changed.

           :param path:
               The path of the remote file or directory to set attributes for
           :param attrs:
               File attributes to set
           :type path: `bytes`
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("setstat is not supported")

    def lsetstat(self, path: bytes, attrs: asyncssh.SFTPAttrs) -> MaybeAwait[None]:
        """Set attributes of a file, directory, or symlink

           This method sets attributes of a file, directory, or symlink.
           A subset of the fields in `attrs` can be initialized and only
           those attributes should be changed.

           :param path:
               The path of the remote file or directory to set attributes for
           :param attrs:
               File attributes to set
           :type path: `bytes`
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("setstat is not supported")

    def fsetstat(self, file_obj: object, attrs: asyncssh.SFTPAttrs) -> MaybeAwait[None]:
        """Set attributes of an open file

           :param file_obj:
               The file to set attributes for
           :param attrs:
               File attributes to set on the file
           :type file_obj: file
           :type attrs: :class:`SFTPAttrs`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("setstat is not supported")

    @override
    def scandir(self, path: bytes):
        """Return names and attributes of the files in a directory

           This function returns an async iterator of :class:`SFTPName`
           entries corresponding to files in the requested directory.

           :param path:
               The path of the directory to scan
           :type path: `bytes`

           :returns: An async iterator of :class:`SFTPName`

           :raises: :exc:`SFTPError` to return an error to the client

        """
        raise asyncssh.SFTPOpUnsupported("scandir is not supported")

    @override
    def remove(self, path: bytes) -> MaybeAwait[None]:
        """Remove a file or symbolic link

           :param path:
               The path of the file or link to remove
           :type path: `bytes`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("remove is not supported")
    
    @override
    def mkdir(self, path: bytes, attrs: asyncssh.SFTPAttrs) -> None:
        """
        Ignore mkdir operations, since the annex doesn't care about directories.
        
        The client has to be compatible with a normal SFTP server, so it will
        try to create directories before uploading files. The server can just
        ignore these operations.
        """
        return

    @override
    def rmdir(self, path: bytes) -> MaybeAwait[None]:
        """
        Ignore rmdir operations, since the annex doesn't care about directories.
        """
        return

    @override
    def rename(self, oldpath: bytes, newpath: bytes) -> MaybeAwait[None]:
        """Rename a file, directory, or link

           This method renames a file, directory, or link.

           .. note:: This is a request for the standard SFTP version
                     of rename which will not overwrite the new path
                     if it already exists. The :meth:`posix_rename`
                     method will be called if the client requests the
                     POSIX behavior where an existing instance of the
                     new path is removed before the rename.

           :param oldpath:
               The path of the file, directory, or link to rename
           :param newpath:
               The new name for this file, directory, or link
           :type oldpath: `bytes`
           :type newpath: `bytes`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("rename is not supported")

    @override
    def readlink(self, path: bytes) -> MaybeAwait[bytes]:
        """Return the target of a symbolic link

           :param path:
               The path of the symbolic link to follow
           :type path: `bytes`

           :returns: bytes containing the target path of the link

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("readlink is not supported")

    @override
    def symlink(self, oldpath: bytes, newpath: bytes) -> MaybeAwait[None]:
        """Create a symbolic link

           :param oldpath:
               The path the link should point to
           :param newpath:
               The path of where to create the symbolic link
           :type oldpath: `bytes`
           :type newpath: `bytes`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("symlink is not supported")

    @override
    def link(self, oldpath: bytes, newpath: bytes) -> MaybeAwait[None]:
        """Create a hard link

           :param oldpath:
               The path of the file the hard link should point to
           :param newpath:
               The path of where to create the hard link
           :type oldpath: `bytes`
           :type newpath: `bytes`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("link is not supported")

    @override
    def lock(self, file_obj: object, offset: int, length: int,
             flags: int) -> MaybeAwait[None]:
        """Acquire a byte range lock on an open file"""

        raise asyncssh.SFTPOpUnsupported('Byte range locks not supported')

    @override
    def unlock(self, file_obj: object, offset: int,
               length: int) -> MaybeAwait[None]:
        """Release a byte range lock on an open file"""

        raise asyncssh.SFTPOpUnsupported('Byte range locks not supported')

    @override
    def posix_rename(self, oldpath: bytes, newpath: bytes) -> MaybeAwait[None]:
        """Rename a file, directory, or link with POSIX semantics

           This method renames a file, directory, or link, removing
           the prior instance of new path if it previously existed.

           :param oldpath:
               The path of the file, directory, or link to rename
           :param newpath:
               The new name for this file, directory, or link
           :type oldpath: `bytes`
           :type newpath: `bytes`

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported("posix_rename is not supported")

    @override
    def statvfs(self, path: bytes):
        """Get attributes of the file system containing a file

           :param path:
               The path of the file system to get attributes for
           :type path: `bytes`

           :returns: An :class:`SFTPVFSAttrs` or an os.statvfs_result
                     containing the file system attributes

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported('statvfs not supported')

    @override
    def fstatvfs(self, file_obj: object):
        """Return attributes of the file system containing an open file

           :param file_obj:
               The open file to get file system attributes for
           :type file_obj: file

           :returns: An :class:`SFTPVFSAttrs` or an os.statvfs_result
                     containing the file system attributes

           :raises: :exc:`SFTPError` to return an error to the client

        """

        raise asyncssh.SFTPOpUnsupported('fstatvfs not supported')

    def fsync(self, file_obj: object) -> MaybeAwait[None]:
        """Force file data to be written to disk

           :param file_obj:
               The open file containing the data to flush to disk
           :type file_obj: file

           :raises: :exc:`SFTPError` to return an error to the client

        """
        # TODO: Consider whether fsync should be supported
        pass
    
CHUNK_SIZE = 8092

class NewFileHandle:
    """A file handle for uploading a new key.
    
    On creation, the file is created in a temporary location.
    After the file is closed, the correct path is computed and the file is moved to the
    final location. This both prevents partial writes and also allows for the file contents
    to be verified before moving it into the annex."""

    # SFTPHandle checks for this attribute and uses it for IO
    writefile: BufferedWriter

    key: AnnexKey
    suffix: str

    cas: ContentAddressableStorage

    def __init__(self, temp_fs: FileSystem, cas: ContentAddressableStorage, key: AnnexKey):
        self.temp_fs = temp_fs
        self.cas = cas
        self.key = key
        self.suffix = pathlib.Path(str(key)).suffix[1:]  # Remove the leading dot
        self.writefile = tempfile.NamedTemporaryFile(dir=self.temp_fs.getsyspath('/'), delete=False, suffix=self.suffix, buffering=CHUNK_SIZE) # type: ignore
        
    def write(self, data: Buffer, /) -> int:
        return self.writefile.write(data)
    
    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self.writefile.seek(offset, whence)
    
    def read(self, size: int = -1) -> bytes:
        raise NotImplementedError("Read not supported on NewFileHandle")

    def file_info(self) -> FileInfo:
        return FileInfo(size=self.writefile.tell())

    def close(self) -> None:
        self.writefile.close()

    def __enter__(self) -> 'NewFileHandle':
        return self
    
    def __exit__(self, type: Optional[type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        self.close()
