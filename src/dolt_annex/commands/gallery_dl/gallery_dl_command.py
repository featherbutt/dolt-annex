#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dataclasses
import json
import logging
from plumbum import cli

from dolt_annex.commands import SubCommand
from dolt_annex.datatypes.config.gallerydl_config import GalleryDLConfig
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.gallery_dl_plugin import make_default_schema, run_gallery_dl

logger = logging.getLogger(__name__)

class GalleryDL(SubCommand):
    """Downlad files using gallery-dl and import them into dolt-annex"""

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

    capture_output = cli.Flag(
        "--capture-output",
        help="If set, capture gallery-dl's stdout and stderr and include them in the output JSON",
        default=False,
    )

    skip_download = cli.Flag(
        "--skip-download",
        help="If set, don't download the submission data and only process the metadata",
        default=False,
    )

    async def main(self, *args) -> int:
        """Entrypoint for gallery-dl command"""
        gallery_dl_config: GalleryDLConfig = self.config.gallery_dl
        gallery_dl_config.capture_output = self.capture_output
        gallery_dl_config.skip_download = self.skip_download
                                                          
        dataset_name = self.dataset

        dataset_schema = DatasetSchema.load(dataset_name)
        if not dataset_schema:
            # Initialize the dataset if it doesn't exist
            logger.info("Dataset %s not found, creating with default schema.", dataset_name)
            dataset_schema = make_default_schema(dataset_name)
            dataset_schema.save()
            
        async with Repo.open(self.config, self.repo) as repo:
            output = await run_gallery_dl(self.config, repo, gallery_dl_config, dataset_schema, *args)
        print(json.dumps(dataclasses.asdict(output), indent=2))
        return 0
    