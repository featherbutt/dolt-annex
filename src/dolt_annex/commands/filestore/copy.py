#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from plumbum import cli # type: ignore

from dolt_annex.logger import logger
from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.cas import filestore_copy

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
        
    async def main(self, *args) -> int:

        from_repo = RepoModel.must_load(self.from_repo)
        to_repo = RepoModel.must_load(self.to_repo)
        async with (
            from_repo.filestore.open(self.config) as from_filestore,
            to_repo.filestore.open(self.config) as to_filestore,
        ):
            for file_key in args or sys.stdin.readlines():
                queried_key = FileKey.must_parse(bytes(file_key.strip(), encoding='utf-8'))
                result = await filestore_copy(
                    src=from_filestore,
                    dst=to_filestore,
                    key=queried_key
                )
                await result.wait_for_complete()
                logger.info(f"Copied key {queried_key} from repo {from_repo.name} to repo {to_repo.name}")
        
        return 0