#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from plumbum import cli

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey

logger = logging.getLogger(__name__)

class Remove(SubCommand):
    """
    Remove a file from a filestore.

    Note that depending on the filestore type, removing a file does not necessarily delete it from the underlying storage.
    """

    parent: CommandGroup

    # TODO: Allow specifying a repo by UUID in addition to name
    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The repo to remove files in",
        mandatory=True
    )
        
    async def main(self, *args) -> int:
        async with Repo.open(self.config, self.repo) as repo:
            for file_key in args or sys.stdin.readlines():
                queried_key = FileKey.must_parse(bytes(file_key.strip(), encoding='utf-8'))
                repo.filestore.file_store.delete(queried_key)
            
        return 0