#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dolt_annex.commands import CommandGroup, SubCommand
from dolt_annex.commands.filestore import display, export, make_alias, remove, verify
from dolt_annex.commands.filestore import migrate

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
FilestoreSubcommand.subcommand("display", display.Display)
FilestoreSubcommand.subcommand("migrate", migrate.Migrate)
FilestoreSubcommand.subcommand("verify", verify.Verify)
FilestoreSubcommand.subcommand("make-alias", make_alias.MakeAlias)
FilestoreSubcommand.subcommand("remove-file", remove.Remove)

