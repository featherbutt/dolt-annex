#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dolt_annex.commands import CommandGroup, SubCommand

from . import insert_record, remove_record, remove_records, read_table, diff

class DatasetSubcommand(CommandGroup, SubCommand):
    """
    Low-level commands for interacting with datasets.
    
    These commands are primarily intended for testing and debugging.
    """

DatasetSubcommand.subcommand("insert-record", insert_record.InsertRecord)
DatasetSubcommand.subcommand("remove-record", remove_record.RemoveRecord)
DatasetSubcommand.subcommand("remove-records", remove_records.RemoveRecords)
DatasetSubcommand.subcommand("read-table", read_table.ReadTable)
DatasetSubcommand.subcommand("diff", diff.Diff)
