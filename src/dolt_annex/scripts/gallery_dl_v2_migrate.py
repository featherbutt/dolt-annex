#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Older versions of dolt-annex used an incorrect database schema for gallery-dl datasets:
- The submissions table mistakenly did not use the submission part in the primary key,
  resulting in only one part of multi-part submissions being saved.
- Both the submissions and metadata tables disambiguated multiple versions of a submission via a
  "last-updated timestamp" column in the primary key, but this value did not exist for all sources.

The new schema instead uses the metadata filekey as part of the primary key in both tables.

Datasets using the old schema can be migrated to the new schema using this script.
"""

import logging
from typing_extensions import Literal
from plumbum import cli

from dolt_annex.commands import SubCommand
from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.replicated_db.interface import TableFilter

logger = logging.getLogger(__name__)

class GalleryDLMigrate(SubCommand):

    old_dataset = cli.SwitchAttr(
        "--old-dataset",
        str,
        help="The name of the dataset being migrated",
    )

    new_dataset = cli.SwitchAttr(
        "--new-dataset",
        str,
        help="The name of the dataset to hold the migrated data",
    )

    repo = cli.SwitchAttr(
        "--repo",
        str,
        help="If set, use the specified repo instead of the default repo",
    )

    async def main(self, *args) -> Literal[0,1]:
        old_dataset_schema = DatasetSchema.must_load(self.old_dataset)
        new_dataset_schema = DatasetSchema.must_load(self.new_dataset)
        repo_model = RepoModel.open(self.parent.config, self.repo)
        repo_id = repo_model.uuid
        async with (
            as_acm(DatabaseConnection.open(self.parent.config)) as conn,
            as_acm(conn.open_dataset(old_dataset_schema)) as old_dataset,
            as_acm(conn.open_dataset(new_dataset_schema)) as new_dataset,
            old_dataset.with_repo(repo_id) as old_dataset_repo,
            new_dataset.with_repo(repo_id) as new_dataset_repo,
        ):
            old_metadata_table = old_dataset_repo.get_table("metadata")
            new_metadata_table = new_dataset_repo.get_table("metadata")
            old_submissions_table = old_dataset_repo.get_table("submissions")
            new_submissions_table = new_dataset_repo.get_table("submissions")
            for old_row in old_metadata_table.get_rows():
                source = old_row["source"]
                sid = old_row["id"]
                metadata_file_key = old_row["annex_key"]
                updated_timestamp = old_row["updated"]
                assert isinstance(metadata_file_key, str)
                new_metadata_table_row = TableRow({
                    "source": source,
                    "id": sid,
                    "file_key": metadata_file_key
                })
                await new_metadata_table.insert(new_metadata_table_row)
                old_submission_rows = old_submissions_table.get_rows(
                    filters=[
                        TableFilter("source", source),
                        TableFilter("id", sid),
                        TableFilter("updated", updated_timestamp)
                    ]
                )
                for old_submission_row in old_submission_rows:
                    part = old_submission_row["part"]
                    submission_file_key = old_submission_row["annex_key"]
                    await new_submissions_table.insert(
                        TableRow({
                            "source": source,
                            "id": sid,
                            "metadata_file_key": metadata_file_key,
                            "part": part,
                            "submission_file_key": submission_file_key
                        })
                    )

        return 0

Command = GalleryDLMigrate
