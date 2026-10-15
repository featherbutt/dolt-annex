#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Optional, Tuple

from plumbum import cli

from dolt_annex.commands.sync.sync import Sync

class Push(Sync):
    """Push imported files to a remote repository"""

    @cli.switch(
        "--remote",
        str,
        help="The name of the dolt-annex remote to push files to. An alias for --to",
    )
    def remote(self, name: str):
        self.to_repo_name = name

    def report(self, files_moved: int, from_name: Optional[str], to_name: Optional[str]) -> None:
        print(f"Pushed {files_moved} files to remote {to_name}")
