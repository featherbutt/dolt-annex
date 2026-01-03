from __future__ import annotations
from typing import Optional

from plumbum import cli # type: ignore

from dolt_annex.datatypes.async_utils import MaybeAwaitable
from dolt_annex.datatypes.config import Config

class Command(cli.Application):
    """
    A base class for all commands, including the top-level application, middle-level command groups, and non-group subcommands.
    """

    # The root application, set automatically by plumbum.
    root_app: CommandGroup

class CommandGroup(Command):
    """
    A base class for all command groups, including the top-level application and middle-level command groups.
    """

    # The chosen subcommand, if any. Set automatically by plumbum.
    nested_command: Optional[SubCommand]

    def main(self, *args) -> MaybeAwaitable[int]:
        """
        The entry point for the command.
        """
        if args:
            print(f"Unknown command: {self.executable} {args[0]}")
            return 1
        elif self.nested_command is None:
            self.help()
            return 0
        return 0
    
class SubCommand(Command):
    """
    A base class for all subcommands, including middle-level command groups, but not the top-level application.
    """

    parent: CommandGroup     # The parent command group. Set automatically by plumbum.

    @property
    def config(self) -> Config:
        """The configuration, initialized from the root application."""
        return self.root_app.config