import json

from plumbum import cli
from dolt_annex.datatypes.async_utils import maybe_await

from dolt_annex.datatypes.repo import Repo
from dolt_annex.application import Application
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import copy, filestore_copy

class Copy(cli.Application):
    """Copy one or more files from one repo to another."""

    parent: Application

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
            from_repo.filestore.open(self.parent.config),
            to_repo.filestore.open(self.parent.config),
        ):
            await filestore_copy(from_repo.filestore, to_repo.filestore, queried_key)
        
        return 0