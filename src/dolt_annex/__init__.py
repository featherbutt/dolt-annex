#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from dolt_annex.commands import gallery_dl_command, init, server_command, import_command, script
from dolt_annex.commands.config import create
from dolt_annex.commands.sync import push, pull
from dolt_annex.commands.dataset import DatasetSubcommand
from dolt_annex.commands.filestore import FilestoreSubcommand

from dolt_annex.datatypes.async_types import maybe_await
from .application import Application

# gallery-dl postprocessor callbacks must be in the top level package, so we import them here
from .gallery_dl_plugin.postprocessors import gallery_dl_post, gallery_dl_prepare, gallery_dl_after
from .gallery_dl_plugin.test_postprocessors import gallery_dl_post_test

Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)
Application.subcommand("gallery-dl", gallery_dl_command.GalleryDL)
Application.subcommand("dataset", DatasetSubcommand)
Application.subcommand("create", create.Create)
Application.subcommand("filestore", FilestoreSubcommand)
Application.subcommand("script", script.Script)

def main(entrypoint=Application):
    """Entry point for dolt-annex package"""
    async def run():
        _, continuation = entrypoint.run(exit=False)
        await maybe_await(continuation)
    asyncio.run(run())

__all__ = ["gallery_dl_post", "gallery_dl_prepare", "gallery_dl_after", "gallery_dl_post_test"]
