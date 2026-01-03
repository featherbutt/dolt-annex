from plumbum import cli # type: ignore

from dolt_annex.commands import CommandGroup, SubCommand

from . import insert_record, read_table

class DatasetSubcommand(CommandGroup, SubCommand):
    """
    Low-level commands for interacting with datasets.
    
    These commands are primarily intended for testing and debugging.
    """

DatasetSubcommand.subcommand("insert-record", insert_record.InsertRecord)
DatasetSubcommand.subcommand("read-table", read_table.ReadTable)