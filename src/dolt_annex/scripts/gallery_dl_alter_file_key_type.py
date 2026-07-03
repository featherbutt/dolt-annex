#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script rewrites the file keys to use a specified file key type.
"""

import logging
from typing_extensions import Literal, List
from plumbum import cli

from dolt_annex.commands import SubCommand
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys import get_file_key_type
from dolt_annex.file_keys.base import FileKey
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import TableFilter

logger = logging.getLogger(__name__)

class AlterFileKeyType(SubCommand):

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

    file_key_type = cli.SwitchAttr(
        "--file-key-type",
        str,
        help="The type of file key to convert to",
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
            TargetFileKeyType = get_file_key_type(self.file_key_type)
            metadata_table = dataset_repo.get_table("metadata")
            submissions_table = dataset_repo.get_table("submissions")
            for old_metadata_row in metadata_table.get_rows(filters=self.filters):
                old_metadata_file_key = FileKey.must_parse(old_metadata_row["file_key"])
                new_metadata_file_keys = await repo.filestore.create_aliases(old_metadata_file_key, new_key_types=[TargetFileKeyType])
                assert len(new_metadata_file_keys) == 1, f"Expected exactly one new metadata file key, got {len(new_metadata_file_keys)}"
                new_metadata_file_key = new_metadata_file_keys[0]
                if new_metadata_file_key == old_metadata_file_key:
                    print(f"File key for metadata row {old_metadata_row} is already of type {self.file_key_type}, skipping.")
                    continue
                await metadata_table.insert(TableRow({
                    "source": old_metadata_row["source"],
                    "id": old_metadata_row["id"],
                    "file_key": str(new_metadata_file_keys[0])
                }))

                for old_submission_row in submissions_table.get_rows(filters=[
                    TableFilter("source", old_metadata_row["source"]),
                    TableFilter("id", old_metadata_row["id"]),
                    TableFilter("metadata_file_key", str(old_metadata_file_key))
                ]):
                    old_submission_file_key = FileKey.must_parse(old_submission_row["submission_file_key"])
                    new_submission_file_keys = await repo.filestore.create_aliases(old_submission_file_key, new_key_types=[TargetFileKeyType])
                    assert len(new_submission_file_keys) == 1, f"Expected exactly one new submission file key, got {len(new_submission_file_keys)}"
                    new_submission_file_key = new_submission_file_keys[0]
                
                    await submissions_table.insert(TableRow({
                        "source": old_submission_row["source"],
                        "id": old_submission_row["id"],
                        "metadata_file_key": new_metadata_file_key,
                        "part": old_submission_row["part"],
                        "submission_file_key": new_submission_file_key,
                    }))
                    await submissions_table.remove(old_submission_row)
                await metadata_table.remove(old_metadata_row)

        return 0

Command = AlterFileKeyType