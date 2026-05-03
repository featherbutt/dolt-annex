#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
If a gallery-dl source changes to strip additional fields from its metadata, you can run
this script to update existing downloads. It iterates over the metadata table and recomputes
the metadata JSON. If it changes, it updates the metadata table, and then also updates the
submissions table to use the new file key as the submissions table key.
"""

import json
import logging
from typing_extensions import Literal
from plumbum import cli

from dolt_annex.application import parse_args
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.gallery_dl_plugin.postprocessors import insert_metadata
from dolt_annex.gallery_dl_plugin.sources import remove_metadata, category_to_source
from dolt_annex.replicated_db.dolt import DatabaseConnection

logger = logging.getLogger(__name__)

application, tailargs = parse_args()

class StripMetadataCommand(cli.Application):
    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of database rows to update at once",
        default=1000,
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset being imported into",
        default="gallery-dl",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, use the specified repo instead of the default repo",
    )

    async def main(self, *args) -> Literal[0,1]:
        # This command is not finished yet.
        return 1
    
        dataset_name = self.dataset

        dataset_schema = DatasetSchema.must_load(dataset_name)

        async with (
            Repo.open(self.parent.config, self.repo) as repo,
            as_acm(DatabaseConnection.open(self.parent.config)) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
            dataset.with_repo(repo.uuid) as dataset_repo,
        ):
            metadata_table = dataset_repo.get_table("metadata")
            for row in metadata_table.get_rows():
                metadata_file_key = row["file_key"]
                assert isinstance(metadata_file_key, str)
                metadata_bytes = await repo.filestore.get_file_bytes(FileKey.must_parse(metadata_file_key))
                metadata = json.loads(metadata_bytes)
                new_metadata = remove_metadata(metadata)
                category = new_metadata["category"]
                source = category_to_source[category]
                if new_metadata != metadata:
                    await insert_metadata(new_metadata, source, dataset_repo, repo)
                    # TODO: Replace instead of insert
        return 0

Command = StripMetadataCommand