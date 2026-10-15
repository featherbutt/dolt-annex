#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import inspect
import sys
import setproctitle

from dolt_annex import interactive
from dolt_annex.commands.application import Application
from dolt_annex.commands import init, server_command, script
from dolt_annex.commands.config import create
from dolt_annex.commands.gallery_dl import gallery_dl_command
from dolt_annex.commands.sync import sync, push, pull
from dolt_annex.commands.dataset import DatasetSubcommand
from dolt_annex.commands.filestore import FilestoreSubcommand
from dolt_annex.constants import PROGNAME
from dolt_annex.datatypes.errors import DoltAnnexApplicationError

# gallery-dl postprocessor callbacks must be in the top level package, so we import them here
from .gallery_dl_plugin.postprocessors import gallery_dl_post, gallery_dl_prepare, gallery_dl_after
from .gallery_dl_plugin.test_postprocessors import gallery_dl_post_test

Application.subcommand("init", init.Init)
Application.subcommand("sync", sync.Sync)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)
Application.subcommand("gallery-dl", gallery_dl_command.GalleryDL)
Application.subcommand("dataset", DatasetSubcommand)
Application.subcommand("create", create.Create)
Application.subcommand("filestore", FilestoreSubcommand)
Application.subcommand("script", script.Script)

def main():
    """Entry point for dolt-annex package"""
    try:
        asyncio.run(async_main())
    except* KeyboardInterrupt:
        print("Exiting...")

async def async_main():
    setproctitle.setproctitle(PROGNAME)
    try:
        # If no command-line arguments are provided, launch the interactive shell.
        # TODO: The interactive shell takes command line arguments which can't be passed in here.
        if len(sys.argv) == 1:
            return await interactive.main()

        # Otherwise, run the command-line application.
        # Most commands are async and return an awaitable continuation instead of a status code.
        _, continuation = Application.run(exit=False)
        if inspect.isawaitable(continuation):
            return await continuation
        else:
            return continuation
    except* DoltAnnexApplicationError as e:
        for exc in e.exceptions:
            print(f"Error: {exc}")
        sys.exit(1)

__all__ = ["gallery_dl_post", "gallery_dl_prepare", "gallery_dl_after", "gallery_dl_post_test"]

if __name__ == "__main__":
    main()