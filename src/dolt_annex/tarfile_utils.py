#!/usr/bin/env python
# -*- coding: utf-8 -*-``

from tarfile import TarFile, TarInfo, BLOCKSIZE, NUL

from dolt_annex.datatypes.async_types import ReadableStream, WritableFileObject
from dolt_annex.filestore.base import copy

async def addfile(tarfile: TarFile, tarinfo: TarInfo, tarfile_fd: WritableFileObject, input_fileobj: ReadableStream):
    """Add the TarInfo object 'tarinfo' to the archive. If 'tarinfo' represents
        a non zero-size regular file, the 'fileobj' argument should be a binary file,
        and tarinfo.size bytes are read from it and added to the archive.
        You can create TarInfo objects directly, or by using gettarinfo().
    """
    buf = tarinfo.tobuf(tarfile.format, tarfile.encoding, tarfile.errors)
    await tarfile_fd.write(buf)
    tarfile.offset += len(buf)

    await copy(src=input_fileobj, dst=tarfile_fd)
    blocks, remainder = divmod(tarinfo.size, BLOCKSIZE)
    if remainder > 0:
        await tarfile_fd.write(NUL * (BLOCKSIZE - remainder))
        blocks += 1
    tarfile.offset += blocks * BLOCKSIZE

    tarfile.members.append(tarinfo)
