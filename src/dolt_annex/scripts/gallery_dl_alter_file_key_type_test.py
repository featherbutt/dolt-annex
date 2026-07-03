#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json
import uuid

import fs.memoryfs
import pytest

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys import Sha256E
from dolt_annex.file_keys.base import FileKey, Sha256HSe
from dolt_annex.filestore.annexfs import AnnexFSModel
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.test_util import run, EnvironmentForTest, test_dataset_schema

@pytest.mark.asyncio
@pytest.mark.parametrize("key_format", [Sha256E])
async def test_alter_file_key_type(tmp_path, setup: EnvironmentForTest):
    """
    Test the basic functionality of the gallery-dl subcommand,
    downloading files using gallery-dl and inserting them into a dolt-annex dataset.
    """
    await run(
        args=["dolt-annex", "init"],
    )
    await run(
        args=["dolt-annex", "gallery-dl", "https://www.furaffinity.net/view/63142315/"],
    )
    await run(
        args=[
            "dolt-annex", "script", "gallery_dl_alter_file_key_type", "--",
            "--file-key-type", "SHA256_HSe",
        ],
    )

    dataset_schema = DatasetSchema.must_load("gallery-dl")

    async with (
        as_acm(DatabaseConnection.open(setup.config)) as conn,
        as_acm(conn.open_dataset(dataset_schema)) as dataset,
        dataset.with_repo(setup.local_repo.uuid) as dataset_repo,
    ):
        metadata_table = dataset_repo.get_table("metadata")
        submissions_table = dataset_repo.get_table("submissions")

        assert len(list(metadata_table.get_rows())) == 1
        for row in metadata_table.get_rows():
            file_key = FileKey.must_parse(row["file_key"])
            assert type(file_key) is Sha256HSe
            await setup.local_repo.filestore.verify_file(file_key)

        assert len(list(submissions_table.get_rows())) == 1
        for row in submissions_table.get_rows():
            submission_file_key = FileKey.must_parse(row["submission_file_key"])
            metadata_file_key = FileKey.must_parse(row["metadata_file_key"])
            assert type(submission_file_key) is Sha256HSe
            assert type(metadata_file_key) is Sha256HSe
            await setup.local_repo.filestore.verify_file(submission_file_key)
            await setup.local_repo.filestore.verify_file(metadata_file_key)

