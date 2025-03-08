from plumbum import cli

import db
from logger import logger
from application import Application

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

    def main(self):
        with self.parent.Downloader(None, self.batch_size) as downloader:
            while True:
                urls, num_urls = db.random_batch(self.url_prefix, downloader.dolt_server.cursor, self.batch_size)
                logger.info(f"Discovered {num_urls} unprocessed URLs.")

                if num_urls == 0:
                    return
                
                for url in urls:
                    with logger.section(f"processing {url}"):
                        downloader.download_file(url)
                downloader.dolt_server.garbage_collect()