#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import tempfile
from plumbum import cli
from dolt_annex.commands import CommandGroup, SubCommand

from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import FileKey

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

        async with repo.filestore.open(self.config) as filestore:
            async with filestore.with_file_object(queried_key) as f:
                with tempfile.NamedTemporaryFile(
                    suffix="." + queried_key["extension"], mode="wb"
                ) as temp_file:
                    buffer_size = 1024 * 1024  # 1 MB buffer size
                    while True:
                        buf = await f.read(buffer_size)
                        if not buf:
                            break
                        temp_file.write(buf)
                    temp_file.flush()
                    temp_path = temp_file.name

                    import webbrowser
                    webbrowser.open("file://" + temp_path)
                    input("Press Enter to continue...")
                    return 0
