#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dataclasses
import json
import logging
from typing import cast
from urllib import response
from plumbum import cli
import requests # type: ignore

from dolt_annex.commands import SubCommand
from dolt_annex.commands.gallery_dl.gallery_dl_command import GalleryDL
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.config.gallerydl_config import GalleryDLConfig
from dolt_annex.datatypes.file_io import async_open
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.gallery_dl_plugin.sources import category_to_source
from dolt_annex.gallery_dl_plugin.sources.base import FileMetadata
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import TableFilter

logger = logging.getLogger(__name__)

class DownloadSkipped(SubCommand):
    """
    Download files that were skipped in a previous gallery-dl run. This command will look for rows in the
    metadata table that don't have the expected number of corresponding rows in the submissions table,
    and will attempt to download those files and insert them into the submissions table.
    """

    parent: GalleryDL

    async def main(self, *args) -> int:
        if args:
            print("This command does not take positional arguments")
            return 1
        dataset_schema = DatasetSchema.must_load(self.parent.dataset)

        async with (
            Repo.open(self.config, self.parent.repo) as repo,
            as_acm(DatabaseConnection.open(self.config)) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
            dataset.with_repo(repo.uuid) as repo_dataset,
        ):
            metadata_table = repo_dataset.get_table("metadata")
            submissions_table = repo_dataset.get_table("submissions")
            for metadata_row in metadata_table.get_rows():
                metadata_file_key = metadata_row["file_key"]
                async with repo.filestore.file_store.get_file_object(metadata_file_key) as metadata_fd:
                    metadata_json = cast(FileMetadata, json.load(metadata_fd))
                    
                source = category_to_source[metadata_json["category"]]
                # Look at IB for comparison
                urls = source.file_url_from_metadata(metadata_json)
                if urls:
                    for i, url in enumerate(urls):
                        # Check if the file is already in the submissions table
                        if (submission_row := submissions_table.get_row(
                            filters=[
                                TableFilter("source", source.source_name),
                                TableFilter("id", metadata_row.get("id")),
                                TableFilter("metadata_file_key", metadata_file_key),
                                TableFilter("page_number", i),
                            ]
                        )):
                            continue
                        logger.info("Downloading skipped file from URL: %s", url)
                        with requests.get(url, stream=True) as response:
                            response.raise_for_status()
                            # Using raw might not work for deflated text.
                            repo.filestore.put_file_object(
                                async_open(response.raw),
                                # TODO: We need the key before we can write?
                                # We can if we use the key from the metadata.
                                

                        
                    

        return 0