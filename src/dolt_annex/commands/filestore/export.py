#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import aiofiles
from plumbum import cli # type: ignore
from dolt_annex.commands import CommandGroup, SubCommand

from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import copy

class Export(SubCommand):
    """Writes a file to stdout given its file key."""

    parent: CommandGroup

    file_key = cli.SwitchAttr(
        "--file-key",
        str,
        help="The file key to look up. If unset, reads the file key from stdin.",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, read from this repo instead of the default",
    )

    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1

        file_key = self.file_key or sys.stdin.readline().strip()
        
        queried_key = FileKey.must_parse(bytes(file_key, encoding='utf-8'))
        if self.repo:
            repo = RepoModel.must_load(self.repo)
        else:
            repo = self.parent.config.get_default_repo()

        async with repo.filestore.open(self.parent.config) as filestore:
            async with filestore.with_file_object(queried_key) as f:
                await copy(src=f, dst=aiofiles.stdout_bytes)

        return 0