#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dataclasses
import json
import logging
from pathlib import Path
import shutil
from plumbum import cli # type: ignore

from dolt_annex.application import Application
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.gallery_dl_plugin import make_default_schema, run_gallery_dl, skip_db_path

logger = logging.getLogger(__name__)

class GalleryDL(cli.Application):
    """Downlad files using gallery-dl and import them into dolt-annex"""

    parent: Application

    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of files to process at once",
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

    capture_output = cli.Flag(
        "--capture-output",
        help="If set, capture gallery-dl's stdout and stderr and include them in the output JSON",
        default=False,
    )

    async def main(self, *args) -> int:
        """Entrypoint for gallery-dl command"""
        dataset_name = self.dataset

        dataset_schema = DatasetSchema.load(dataset_name)
        if not dataset_schema:
            # Initialize the dataset if it doesn't exist
            logger.info("Dataset %s not found, creating with default schema.", dataset_name)
            dataset_schema = make_default_schema(dataset_name)
            dataset_schema.save()
            
        async with Repo.open(self.parent.config, self.repo) as repo:
            output = await run_gallery_dl(self.parent.config, repo, self.batch_size, dataset_schema, self.capture_output, *args)
        print(json.dumps(dataclasses.asdict(output), indent=2))
        return 0
    