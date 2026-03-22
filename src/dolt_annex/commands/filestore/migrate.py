#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys
from typing import Iterable, cast

import fs.info
from plumbum import cli # type: ignore

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.annexfs import AnnexFS, AnnexFSModel
from dolt_annex.filestore.archivefs import ArchiveFS, ArchiveFSModel
from dolt_annex.filestore.cas import filestore_copy

logger = logging.getLogger(__name__)

class Migrate(SubCommand):
    """
    Move files from a AnnexFS filestore to an ArchiveFS filestore.

    It walks the directory tree of the AnnexFS, and for each file:
    - If the file already exists in the ArchiveFS, it checks that both files have the same file hash,
    and that the file key matches the hash. After confirming, it deletes the file.
    - If the file doesn't exist in the ArchiveFS, it copies the file into the ArchiveFS,
    but does not remove the file from the AnnexFS.

    After walking each directory in the AnnexFS, if the directory is now empty, it will be deleted.

    Running this script twice should result in an empty AnnexFS. It avoids deleting files in the same pass that
    it copies them in order to minimize the risk of data loss if the OS is buffering writes.
    This *shouldn't* be an issue, since the ArchiveFS calls fscync after each write, but better safe than sorry.

    In theory, this code should work for destinations other than ArchiveFS. It currently requires ArchiveFS
    to eliminate the possibility of accidentally running this script with the source and destination set to
    the same filestore.
    """

    parent: CommandGroup

    # TODO: Allow specifying a repo by UUID in addition to name
    from_repo = cli.SwitchAttr(
        "--from",
        str,
        help="The repo to copy files from",
        mandatory=True
    )

    # TODO: Allow specifying a repo by UUID in addition to name
    to_repo = cli.SwitchAttr(
        "--to",
        str,
        help="The repo to copy files to",
        mandatory=True
    )

    remove_if_exists = cli.Flag(
        "--remove-if-exists",
        help="remove files from the source filestore if they already exist in the destination filestore"        
    )
        
    async def main(self, *args) -> int:

        from_repo = RepoModel.must_load(self.from_repo)
        to_repo = RepoModel.must_load(self.to_repo)
        assert isinstance(from_repo.filestore, AnnexFSModel)
        assert isinstance(to_repo.filestore, ArchiveFSModel)
        to_repo = RepoModel.must_load(self.to_repo)
        async with (
            from_repo.filestore.open(self.config) as from_filestore,
            to_repo.filestore.open(self.config) as to_filestore,
        ):
            assert isinstance(from_filestore, AnnexFS)
            assert isinstance(to_filestore, ArchiveFS)
            
            for walker in from_filestore.file_system.walk(search="depth"):
                root = cast(str, walker.path)
                files = cast(Iterable[fs.info.Info], walker.files)
                dirs = cast(Iterable[fs.info.Info], walker.dirs)
                for file in files:
                    file_key = FileKey.must_parse(file.name.encode("utf-8"))
                    file_path = file.make_path(root)
                    if to_filestore.exists(file_key):
                        await from_filestore.verify_file(file_key)
                        await to_filestore.verify_file(file_key)
                        
                        if self.remove_if_exists:
                            logger.info("%s exists in destination store, removing", file_key)
                            from_filestore.file_system.remove(file_path)
                        else:
                            logger.info("%s exists in destination store, skipping", file_key)
                    else:
                        logger.info("%s does not exist in destination store, copying", file_key)
                        result = await filestore_copy(
                            src=from_filestore,
                            dst=to_filestore,
                            key=file_key
                        )
                        await result.wait_for_complete()

        return 0