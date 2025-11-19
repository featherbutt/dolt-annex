#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dolt_annex.commands import gallery_dl_command, insert_record, init, server_command, push, pull, import_command
from .application import Application

# gallery-dl postprocessors callbacks must be in the top level package, so we import them here
from .gallery_dl.postprocessors import gallery_dl_post, gallery_dl_prepare, gallery_dl_after

Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)
Application.subcommand("gallery-dl", gallery_dl_command.GalleryDL)
Application.subcommand("insert-record", insert_record.InsertRecord)

if __name__ == "__main__":
    Application.run()

def main():
    Application.run()