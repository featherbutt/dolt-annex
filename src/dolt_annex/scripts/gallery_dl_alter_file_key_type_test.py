#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json
import uuid

import fs.memoryfs
import pytest

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
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

    dataset_schema = DatasetSchema.must_load("gallery-dl")

    async with (
        as_acm(DatabaseConnection.open(setup.config)) as conn,
        as_acm(conn.open_dataset(dataset_schema)) as dataset,
        dataset.with_repo(setup.local_repo.uuid) as dataset_repo,
    ):
        metadata_table = dataset_repo.get_table("metadata")
        submissions_table = dataset_repo.get_table("submissions")

        # To test the outcome where only the submission needs to be rewritten,
        # insert an extra row into the submissions table.
        extra_metadata_key = Sha256HSe.from_bytes(b"metadata")
        extra_submission_key = Sha256E.from_bytes(b"submission")
        await submissions_table.insert(TableRow({
            "source": "test",
            "id": 1,
            "metadata_file_key": bytes(extra_metadata_key),
            "part": 1,
            "submission_file_key": bytes(extra_submission_key)
        }))
        await submissions_table.flush()

    await run(
        args=[
            "dolt-annex", "script", "gallery_dl_alter_file_key_type", "--",
            "--file-key-type", "SHA256_HSe",
        ],
    )

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

        assert len(list(submissions_table.get_rows())) == 2
        for row in submissions_table.get_rows():
            submission_file_key = FileKey.must_parse(row["submission_file_key"])
            metadata_file_key = FileKey.must_parse(row["metadata_file_key"])
            assert type(submission_file_key) is Sha256HSe
            assert type(metadata_file_key) is Sha256HSe
            await setup.local_repo.filestore.verify_file(submission_file_key)
            await setup.local_repo.filestore.verify_file(metadata_file_key)

