from plumbum import cli # type: ignore

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.commands.filestore import export

from . import insert, whereis, copy

class FilestoreSubcommand(CommandGroup, SubCommand):
    """
    Low-level commands for interacting directly with filestores.
    
    These commands are primarily intended for testing and debugging.
    """

FilestoreSubcommand.subcommand("insert-file", insert.Insert)
FilestoreSubcommand.subcommand("whereis", whereis.WhereIs)
FilestoreSubcommand.subcommand("copy", copy.Copy)
FilestoreSubcommand.subcommand("export-file", export.Export)
