import importlib
import logging
from typing_extensions import Literal


from dolt_annex.commands import CommandGroup, SubCommand

logger = logging.getLogger(__name__)

class Script(SubCommand):
    """
    Run a custom sub-command in the scripts directory.
    
    This is useful for niche commands that leverage dolt-annex as a library,
    but don't belong in the list of main commands.

    By using namespace packages, users can add commands without needing
    to touch dolt-annex's install location, while still getting the benefit of
    top-level command line flags.
    """

    parent: CommandGroup
    
    def main(self, script_name: str, *argv: str) -> Literal[0, 1]:
        script_module = importlib.import_module(f"dolt_annex.scripts.{script_name}")
        subcommand = getattr(script_module, "Command", None)
        if subcommand is None:
            logger.error("Could not find script %s", script_name)
            return 1
        self.nested_command = (
                    subcommand,
                    [self.PROGNAME + " " + script_name, *argv],
                )
        return 0
