#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .commands import init, server_command, sync, push, pull, import_command
from .application import Application

Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("sync", sync.Sync)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)

if __name__ == "__main__":
    Application.run()

def main():
    Application.run()