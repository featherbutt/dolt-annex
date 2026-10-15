#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Optional, Tuple

from plumbum import cli

from dolt_annex.commands.sync.sync import Sync

class Pull(Sync):
    """Pull imported files from a remote repository"""

    @cli.switch(
        "--remote",
        str,
        help="The name of the dolt-annex remote to pull files from. An alias for --from",
    )
    def remote(self, name: str):
        self.from_repo_name = name

    def report(self, files_moved: int, from_name: Optional[str], to_name: Optional[str]) -> None:
        print(f"Pulled {files_moved} files from remote {from_name}")
