#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Iterable, cast

import fs.info
from plumbum import cli # type: ignore

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.archivefs import ArchiveFS
from dolt_annex.filestore.cas import ContentAddressableStorageKeyMismatchError, filestore_copy

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
    remove_corrupt_files = cli.Flag(
        "--remove-corrupt-files",
        help="remove files from the source filestore if they are corrupt"        
    )
        
    async def main(self, *args) -> int:

        async with (
            Repo.open(self.config, self.from_repo) as from_repo,
            Repo.open(self.config, self.to_repo) as to_repo,
        ):
            assert isinstance(from_repo.filestore.file_store, AnnexFS)
            assert isinstance(to_repo.filestore.file_store, ArchiveFS)

            for walker in from_repo.filestore.file_store.file_system.walk(search="depth"):
                root = cast(str, walker.path)
                files = cast(Iterable[fs.info.Info], walker.files)
                dirs = cast(Iterable[fs.info.Info], walker.dirs)

                has_dirs = False
                for _ in dirs:
                    has_dirs = True
                    break

                can_remove_dir = True
                
                for file in files:
                    file_key = FileKey.must_parse(file.name.encode("utf-8"))
                    file_path = file.make_path(root)
                    if not await from_repo.filestore.contains_valid_file(file_key):
                        if self.remove_corrupt_files:
                            logger.info("%s is corrupt in the source store, removing", file_key)
                            from_repo.filestore.file_store.file_system.remove(file_path)
                        else:
                            logger.info("%s is corrupt in the source store, skipping", file_key)
                            can_remove_dir = False
                        continue
                    if await maybe_await(to_repo.filestore.file_store.exists(file_key)):
                        await to_repo.filestore.verify_file(file_key)

                        if self.remove_if_exists:
                            logger.info("%s exists in destination store, removing", file_key)
                            from_repo.filestore.file_store.file_system.remove(file_path)
                        else:
                            logger.info("%s exists in destination store, skipping", file_key)
                            can_remove_dir = False
                        continue
                    else:
                        logger.info("%s does not exist in destination store, copying", file_key)
                        result = await filestore_copy(
                            src=from_repo.filestore,
                            dst=to_repo.filestore,
                            key=file_key
                        )
                        await result.wait_for_complete()
                        
                        can_remove_dir = False
                if not has_dirs and can_remove_dir:
                    from_repo.filestore.file_store.file_system.removedir(root)

        return 0