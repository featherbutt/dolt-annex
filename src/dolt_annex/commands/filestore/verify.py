#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from plumbum import cli # type: ignore

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import RepoModel

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
        
    async def main(self, *args) -> int:
        repo = RepoModel.must_load(self.repo)
        async with repo.filestore.open(self.config) as filestore:
            await filestore.verify_all_files()
            logger.info("All files verified successfully.")
            
        return 0