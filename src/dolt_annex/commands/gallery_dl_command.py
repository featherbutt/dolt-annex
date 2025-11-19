#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from plumbum import cli # type: ignore

from dolt_annex.application import Application
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.logger import logger
from dolt_annex.gallery_dl import dataset_context, make_default_schema, run_gallery_dl
from dolt_annex.table import Dataset

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

    def main(self, *args) -> int:
        """Entrypoint for gallery-dl command"""
        asyncio.run(self._main_async(args))
        return 0
    
    async def _main_async(self, args) -> int:
        dataset_name = self.dataset
        dataset_schema = DatasetSchema.load(dataset_name)
        if not dataset_schema:
            # Initialize the dataset if it doesn't exist
            logger.info(f"Dataset {dataset_name} not found, creating with default schema.")
            dataset_schema = make_default_schema(dataset_name)
            dataset_schema.save_as(dataset_name)
            
        async with (
            Dataset.connect(self.parent.config, self.batch_size, dataset_schema) as dataset,
        ):
            dataset_context.set(dataset)
            run_gallery_dl(*args)
        return 0
    