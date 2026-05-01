import logging
from typing import List

from plumbum import cli

from dolt_annex.commands import CommandGroup, SubCommand

logger = logging.getLogger(__name__)

class Script(SubCommand):
    """
    Run a custom sub-command in the scripts directory.
    
    This is useful for niche commands that leverage dolt-annex as a library,
    but don't belong in the list of main commands.

    By specifying the scripts directory, users can add commands without needing
    to touch dolt-annex's install location, while still getting the benefit of
    top-level command line flags.
    """

    parent: CommandGroup

    script_dir = cli.SwitchAttr(
        "--script-dir",
        str,
        help="The directory to search for scripts",
    )
    
    async def main(self, script_name: str, *argv: List[str]) -> Literal[0, 1]:
        return 1
