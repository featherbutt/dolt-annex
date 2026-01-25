import json
import sys

from plumbum import cli # type: ignore
from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.async_utils import maybe_await

from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.file_keys.base import FileKey

class WhereIs(SubCommand):
    """List the repos that contain a file key."""

    parent: CommandGroup

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, limit the search to a single named repo",
    )
        
    async def main(self, *args) -> int:
        for file_key in args or sys.stdin.readlines():
            queried_key = FileKey(bytes(file_key.strip(), encoding='utf-8'))
            locations = []
            if self.repo:
                repo_models = [RepoModel.must_load(self.repo)]
            else:
                repo_models = RepoModel.all()

            for repo in repo_models:
                async with repo.filestore.open(self.parent.config) as filestore:
                    if await maybe_await(filestore.exists(queried_key)):
                        locations.append({"name": repo.name, "uuid": str(repo.uuid)})

            print(json.dumps(locations))
        
        return 0