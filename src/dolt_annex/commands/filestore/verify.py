#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from plumbum import cli

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey

logger = logging.getLogger(__name__)

class Verify(SubCommand):
    """
    Verify that all files in the filestore are correct by checking that their file keys match their contents.

    This is potentially very slow, and is mostly intended for testing and debugging purposes.
    """

    parent: CommandGroup

    # TODO: Allow specifying a repo by UUID in addition to name
    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The repo to verify files in",
        mandatory=True
    )

    all = cli.Flag(
        "--all",
        help="If set, verify all files in the repo.",
        default=False,
    )
        
    async def main(self, *args) -> int:
        async with Repo.open(self.config, self.repo) as repo:
            if self.all:
                await repo.filestore.verify_all_files()
            else:
                for file_key in args or sys.stdin.readlines():
                    queried_key = FileKey.must_parse(bytes(file_key.strip(), encoding='utf-8'))
                    await repo.filestore.verify_file(queried_key)
            logger.info("All files verified successfully.")
            
        return 0