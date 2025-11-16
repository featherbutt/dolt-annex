#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .commands import init, server_command, push, pull, import_command, gallery_dl
from .application import Application

# gallery-dl postprocessors callbacks must be in the top level package, so we import them here
from .gallery_dl.postprocessors import gallery_dl_post, gallery_dl_prepare, gallery_dl_after

Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)
Application.subcommand("gallery-dl", gallery_dl.GalleryDL)

if __name__ == "__main__":
    Application.run()

def main():
    Application.run()