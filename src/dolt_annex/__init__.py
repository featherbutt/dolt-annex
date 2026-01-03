#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from dolt_annex.commands import gallery_dl_command, init, server_command, import_command
from dolt_annex.commands.config import create
from dolt_annex.commands.sync import push, pull
from dolt_annex.commands.dataset import read_table, insert_record
import dolt_annex.commands.filestore

from dolt_annex.datatypes.async_utils import maybe_await
from .application import Application

# gallery-dl postprocessor callbacks must be in the top level package, so we import them here
from .gallery_dl_plugin.postprocessors import gallery_dl_post, gallery_dl_prepare, gallery_dl_after

Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)
Application.subcommand("gallery-dl", gallery_dl_command.GalleryDL)
Application.subcommand("insert-record", insert_record.InsertRecord)
Application.subcommand("read-table", read_table.ReadTable)
Application.subcommand("create", create.Create)
Application.subcommand("filestore", dolt_annex.commands.filestore.FilestoreSubcommand)

def main():
    """Entry point for dolt-annex package"""
    async def run():
        _, continuation = Application.run(exit=False)
        await maybe_await(continuation)
    asyncio.run(run())