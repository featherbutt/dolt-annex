from abc import ABC as AbstractBaseClass, abstractmethod

from plumbum import cli # type: ignore

from dolt_annex.datatypes.async_utils import MaybeAwaitable
from dolt_annex.application import Application

class SubCommand(cli.Application, AbstractBaseClass):

    parent: Application

    @abstractmethod
    def main(self, *args) -> MaybeAwaitable[int]:
        """
        The entry point for the sub-command.
        """
