import os
import shutil
import subprocess
import tempfile
from plumbum import cli # type: ignore

from typing_extensions import Optional

import requests

from downloader import GitAnnexDownloader, move_files
import sql
from logger import logger
from application import Application, Downloader
from type_hints import AnnexKey, PathLike

class DownloadBatch(cli.Application):
    """Download missing files and add them to the annex"""
    
    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The path of files to process at once",
        default = 1000,
    )

    url_prefix = cli.SwitchAttr(
        "--url-prefix",
        str,
        help="Only download files with URLs that start with this prefix",
        default="",
    )

    def main(self, *args):
        """Entrypoint for download command"""
        
        if len(args) > 0:
            print("Unexpected positional arguments: ", args)
            self.help()
            return 1
        
        with Downloader(self.parent.config, self.batch_size) as downloader:
            while True:
                urls, num_urls = sql.random_batch(self.url_prefix, downloader.dolt_server.cursor, self.batch_size)
                logger.info(f"Discovered {num_urls} unprocessed URLs.")

                if num_urls == 0:
                    return
                
                for url in urls:
                    with logger.section(f"processing {url}"):
                        self.download_file(downloader, url)
                downloader.dolt_server.garbage_collect()
        return 0

    def download_file(self, downloader: GitAnnexDownloader, url: str) -> Optional[str]:
        """Download a file from a url, add it to the annex, and update the database"""
        extension = os.path.splitext(url)[1]
        suffix = None
        if len(extension) <= downloader.max_extension_length+1:
            suffix = extension
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                temp_file.flush()
                
                abs_path = os.path.abspath(temp_file.name) 
                key = downloader.git.annex.calckey(abs_path)
                move_files(downloader, shutil.move, {AnnexKey(key): PathLike(abs_path)})  
                
                downloader.add_local_source(key)
                downloader.update_database(url, key)
                
                return key
                
            except (requests.RequestException, subprocess.CalledProcessError, ValueError) as e:
                print(f"Error processing URL {url}: {str(e)}")
                return None
