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
from typing_extensions import Literal, List
from plumbum import cli

from dolt_annex.commands import SubCommand
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.gallery_dl_plugin.postprocessors import insert_metadata, serialize_metadata
from dolt_annex.gallery_dl_plugin.sources import remove_metadata, category_to_source
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import TableFilter

logger = logging.getLogger(__name__)

class UpdateMetadata(SubCommand):
    batch_size = cli.SwitchAttr(
        "--batch_size",
        int,
        help="The number of database rows to update at once",
        default=1000,
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset to update",
        default="gallery-dl",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, use the specified repo instead of the default repo",
    )

    filters: List[TableFilter] = []

    @cli.switch(
        "--where",
        str,
        list = True,
        help="A filter condition on the table rows to be read",
    )
    def where(self, filter_strings: List[str]):
        for filter_string in filter_strings:
            if '=' not in filter_string:
                raise ValueError(f"Invalid filter string: {filter_string}")
            column_name, column_value = filter_string.split('=', maxsplit=1)
            self.filters.append(TableFilter(column_name, column_value))


    async def main(self, *args) -> Literal[0,1]:
        dataset_name = self.dataset

        dataset_schema = DatasetSchema.must_load(dataset_name)

        async with (
            Repo.open(self.config, self.repo) as repo,
            as_acm(DatabaseConnection.open(self.config)) as conn,
            as_acm(conn.open_dataset(dataset_schema)) as dataset,
            dataset.with_repo(repo.uuid) as dataset_repo,
        ):
            metadata_table = dataset_repo.get_table("metadata")
            submissions_table = dataset_repo.get_table("submissions")
            for old_metadata_row in metadata_table.get_rows(filters=self.filters):
                old_metadata_file_key = FileKey.must_parse(old_metadata_row["file_key"])
                metadata_bytes = await repo.filestore.file_store.get_file_bytes(old_metadata_file_key)
                metadata = json.loads(metadata_bytes)
                new_metadata = remove_metadata(metadata)
                category = new_metadata["category"]
                source = category_to_source[category]
                new_metadata_file_key, _ = serialize_metadata(new_metadata, source, repo)
                if new_metadata_file_key != old_metadata_file_key:
                    await insert_metadata(new_metadata, source, dataset_repo, repo)
                    for old_submission_row in submissions_table.get_rows(filters=[
                        TableFilter("source", old_metadata_row["source"]),
                        TableFilter("id", old_metadata_row["id"]),
                        TableFilter("metadata_file_key", str(old_metadata_file_key))
                    ]):
                        await submissions_table.insert(TableRow({
                            "source": old_submission_row["source"],
                            "id": old_submission_row["id"],
                            "metadata_file_key": new_metadata_file_key,
                            "part": old_submission_row["part"],
                            "submission_file_key": old_submission_row["submission_file_key"],
                        }))
                        await submissions_table.remove(old_submission_row)
                    await metadata_table.remove(old_metadata_row)

                    logger.info(f"moving {old_metadata_row} to {new_metadata_file_key}")
                else:
                    logger.info(f"no updates for {old_metadata_row}")

        return 0

Command = UpdateMetadata