#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from plumbum import cli
from dolt_annex.commands import CommandGroup, SubCommand

from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import FileKey
from dolt_annex.server import http

class Display(SubCommand):
    """Displays a file using the default system viewer."""

    parent: CommandGroup

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, read from this repo instead of the default",
    )

    async def main(self, file_key: str = "") -> int:
        file_key = file_key or str(sys.stdin.readline().strip())
        
        queried_key = FileKey.must_parse(bytes(file_key, encoding='utf-8'))
        if self.repo:
            repo = RepoModel.must_load(self.repo)
        else:
            repo = self.config.get_default_repo()

        async with (
            repo.filestore.open(self.config) as filestore,
            http.serve_filestore(filestore) as http_server,
        ):
            await http_server.display_file(queried_key)
            return 0


