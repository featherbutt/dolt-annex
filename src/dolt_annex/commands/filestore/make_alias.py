from contextlib import AsyncExitStack
import json
import sys
import logging

from plumbum import cli # type: ignore
from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.datatypes.async_types import maybe_await

from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.file_keys import FileKeyType, get_file_key_type
from dolt_annex.file_keys.base import FileKey

logger = logging.getLogger(__name__)

class MakeAlias(SubCommand):
    """Create alias keys that point to the same file as an existing key, but with a different key type."""

    parent: CommandGroup

    key_type = cli.SwitchAttr(
        "--key-type",
        str,
        help="The type of key to create. Can be repeated.",
        list=True,
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="The repo to create alias keys in",
    )
        
    async def main(self, *args) -> int:
        async with Repo.open(self.config, self.repo) as repo:
            key_types = [get_file_key_type(key_type_str) for key_type_str in self.key_type]
            if not key_types:
                key_types = repo.alternate_key_formats
            for file_key in args or sys.stdin.readlines():
                queried_key = FileKey.must_parse(bytes(file_key.strip(), encoding='utf-8'))
                if not await maybe_await(repo.filestore.exists(queried_key)):
                    print(f"File with key {queried_key} not found in annex, skipping.", file=sys.stderr)
                    continue
                
                await repo.filestore.create_aliases(queried_key, key_types)
        
        return 0