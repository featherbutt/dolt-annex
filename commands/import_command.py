from collections.abc import Callable
import os
from typing import List

from plumbum import cli, local

import annex
from application import Application
from downloader import GitAnnexDownloader

def url_from_path_other_annex(other_annex_dir: str) -> Callable[[str], List[str]]:
    def inner(path: str) -> List[str]:
        parts = path.split(os.path.sep)
        annex_object_path = '/'.join(parts[-4:-1])
        other_git = local.cmd.git["-C", "./git-annex-from"]
        web_log = other_git('show', f'git-annex:{annex_object_path}.log.web', retcode=None)
        if web_log:
            return annex.parse_web_log(web_log)
        return []
    return inner

def url_from_path_directory(url_prefix: str) -> Callable[[str], List[str]]:
    def inner(path: str) -> List[str]:
        return [f"{url_prefix}/{path}"]
    return inner

def import_(downloader: GitAnnexDownloader, file_or_directory: str, url_from_path: Callable[[str], str]):
        if os.path.isfile(file_or_directory):
            downloader.import_file(file_or_directory, url_from_path)
        elif os.path.isdir(file_or_directory):
            downloader.import_directory(file_or_directory, url_from_path)
        else:
            raise ValueError(f"Path {file_or_directory} is not a file or directory")

class Import(cli.Application):
    """Import a file or directory into the annex and database"""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The path of files to process at once",
        default = 1000,
    )

    from_other_annex = cli.SwitchAttr(
        "--from-other-annex",
        cli.ExistingDirectory,
        help="The path of another git-annex repository to import from",
        excludes = ["--url-prefix"],
    )

    from_directory_path = cli.SwitchAttr(
        "--url-prefix",
        cli.ExistingDirectory,
        help="The path of a directory to import from",
        excludes = ["--from-other-annex"],
    )

    def url_factory(self):
        if self.from_other_annex:
            return url_from_path_other_annex(self.from_other_annex)
        elif self.from_directory_path:
            return url_from_path_directory(self.from_directory_path)
        else:
            return None

    def main(self, *files_or_directories: str):
        url_from_path = self.url_factory()
        with self.parent.Downloader(self.batch_size) as downloader:
            for file_or_directory in files_or_directories:
                import_(downloader, file_or_directory, url_from_path)
