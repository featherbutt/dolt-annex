import json
from math import e

from plumbum import cli

from dolt_annex.datatypes.repo import Repo
from dolt_annex.application import Application
from dolt_annex.file_keys.base import FileKey

class WhereIs(cli.Application):
    """List the repos that contain a file key."""

    parent: Application

    file_key = cli.SwitchAttr(
        "--file-key",
        str,
        help="The file key to look up",
        mandatory = True
    )

    # TODO: Allow specifying a repo by UUID, or leaving blank to search all repos
    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, limit the search to a single named repo",
        mandatory = True
    )
        
    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1

        locations = []
        repo = Repo.must_load(self.repo)
        if repo.filestore.exists(FileKey(bytes(self.file_key, encoding='utf-8'))):
            locations.append({"name": repo.name, "uuid": str(repo.uuid)})

        print(json.dumps(locations))
        
        return 0