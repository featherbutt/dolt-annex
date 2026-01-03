#!/usr/bin/env python
# -*- coding: utf-8 -*-

from plumbum import cli # type: ignore

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import filestore_copy

class Copy(SubCommand):
    """Copy one or more files from one repo to another."""

    parent: CommandGroup

    file_key = cli.SwitchAttr(
        "--file-key",
        str,
        help="The file key to copy",
        mandatory = True
    )

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
        if args:
            print("This command does not take positional arguments")
            return 1

        queried_key = FileKey(bytes(self.file_key, encoding='utf-8'))
        from_repo = Repo.must_load(self.from_repo)
        to_repo = Repo.must_load(self.to_repo)
        async with (
            from_repo.filestore.open(self.config),
            to_repo.filestore.open(self.config),
        ):
            await filestore_copy(
                src=from_repo.filestore,
                dst=to_repo.filestore,
                key=queried_key
            )
        
        return 0