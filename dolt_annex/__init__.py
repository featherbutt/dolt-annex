from .commands import init, server_command, sync, push, pull, scan_annex, download, import_command
from .application import Application

Application.subcommand("download", download.DownloadBatch)
Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("scan-annex", scan_annex.ScanAnnex)
Application.subcommand("sync", sync.Sync)
Application.subcommand("push", push.Push)
Application.subcommand("pull", pull.Pull)
Application.subcommand("server", server_command.Server)

if __name__ == "__main__":
    Application.run()

def main():
    Application.run()