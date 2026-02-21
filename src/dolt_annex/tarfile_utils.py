#!/usr/bin/env python
# -*- coding: utf-8 -*-``

from tarfile import HeaderError, TarFile, TarInfo, BLOCKSIZE, NUL

from dolt_annex.datatypes.async_types import ReadableStream, WritableFileObject
from dolt_annex.filestore.base import copy

def advance_to_end(tarfile: TarFile):
    """
    The standard library tarfile module raises an exception and closes if
    the file does not end with two empty blocks after the last member.

    We want to be more permissive, which means manually advancing to the end of the archive file.
    If we encounter an unexpected EOF or if the file ends with a non-empty block,
    we simply seek to the end of the last member and continue writing from there.
    """
    while True:
        tarfile.fileobj.seek(tarfile.offset)
        previous_offset = tarfile.offset
        try:
            tarinfo = tarfile.tarinfo.fromtarfile(tarfile)
            # Calling fromtarfile advances the offset to the start of the next block.
            # Check that the file is not truncated by attempting to read the last byte of the previous block.
            tarfile.fileobj.seek(tarfile.offset - 1)
            if tarfile.fileobj.read(1) == b'':
                tarfile.fileobj.seek(previous_offset)
                break
            tarfile.members.append(tarinfo)
        except HeaderError:
            tarfile.fileobj.seek(tarfile.offset)
            break

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
