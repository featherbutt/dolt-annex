from ast import Sub
import json

from plumbum import cli # type: ignore
from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.async_utils import maybe_await

from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey

class WhereIs(SubCommand):
    """List the repos that contain a file key."""

    parent: CommandGroup

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
                if await maybe_await(repo.filestore.exists(queried_key)):
                    locations.append({"name": repo.name, "uuid": str(repo.uuid)})

        print(json.dumps(locations))
        
        return 0