import json

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
    )
        
    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1

        queried_key = FileKey(bytes(self.file_key, encoding='utf-8'))
        locations = []
        if self.repo:
            repos = [Repo.must_load(self.repo)]
        else:
            repos = Repo.all()

        for repo in repos:
            async with repo.filestore.open(self.parent.config):
                if repo.filestore.exists(queried_key):
                    locations.append({"name": repo.name, "uuid": str(repo.uuid)})

        print(json.dumps(locations))
        
        return 0