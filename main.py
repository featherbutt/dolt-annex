from commands import init, sync, scan_annex, download, import_command
from application import Application

Application.subcommand("download", download.DownloadBatch)
Application.subcommand("import", import_command.Import)
Application.subcommand("init", init.Init)
Application.subcommand("scan-annex", scan_annex.ScanAnnex)
Application.subcommand("sync", sync.Sync)

if __name__ == "__main__":
    Application.run()