from contextlib import AsyncExitStack
import json
import sys

from plumbum import cli
from dolt_annex.commands import CommandGroup, SubCommand

from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStoreModel

class WhereIs(SubCommand):
    """List the repos that contain a file key."""

    parent: CommandGroup

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, limit the search to a single named repo",
    )
        
    async def main(self, *args) -> int:
        if self.repo:
            filestore_models = [FileStoreModel.must_load(self.repo)]
        else:
            filestore_models = FileStoreModel.all()

        async with AsyncExitStack() as stack:
            filestores = [await stack.enter_async_context(filestore.open(self.config)) for filestore in filestore_models]
            for file_key in args or sys.stdin.readlines():
                locations = []
                queried_key = FileKey(bytes(file_key.strip(), encoding='utf-8'))
                for filestore, repo in zip(filestores, filestore_models):
                    if await filestore.exists(queried_key):
                        locations.append({"name": repo.name, "uuid": str(repo.uuid)})

                locations.sort(key=lambda x: x["name"])
                print(json.dumps(locations))
        
        return 0