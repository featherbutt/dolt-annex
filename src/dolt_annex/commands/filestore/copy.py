#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
from plumbum import cli

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.cas import filestore_copy

logger = logging.getLogger(__name__)

class Copy(SubCommand):
    """Copy one or more files from one repo to another."""

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

    all = cli.Flag(
        "--all",
        help="If set, copy all files from the source repo to the destination repo.",
        default=False,
    )
        
    async def main(self, *args) -> int:
        async with (
            Repo.open(self.config, self.from_repo) as from_repo,
            Repo.open(self.config, self.to_repo) as to_repo,
        ):
            if self.all:
                async for file_key, data_source in from_repo.filestore.file_store.get_files():
                    await to_repo.filestore.put_file_object(data_source, file_key=file_key)
                    logger.info("Copied key %s from repo %s to repo %s", file_key, from_repo.name, to_repo.name)
            else:
                for key_str in args or sys.stdin.readlines():
                    file_key = FileKey.must_parse(bytes(key_str.strip(), encoding='utf-8'))
                    await filestore_copy(
                        src=from_repo.filestore,
                        dst=to_repo.filestore,
                        key=file_key,
                    )
                    logger.info("Copied key %s from repo %s to repo %s", file_key, from_repo.name, to_repo.name)

        return 0