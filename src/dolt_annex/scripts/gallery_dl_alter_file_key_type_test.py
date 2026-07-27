#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pytest

from dolt_annex.datatypes.async_utils import as_acm
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys import Sha256E
from dolt_annex.file_keys.base import FileKey, Sha256HSe
from dolt_annex.replicated_db.dolt import DatabaseConnection
from dolt_annex.test_util import run, EnvironmentForTest

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
    
    # Calling gallery-dl will initialize the dataset, then fail because there are no URLs.
    await run(
        args=["dolt-annex", "gallery-dl"],
        expected_exception=SystemExit
    )

    dataset_schema = DatasetSchema.must_load("gallery-dl")

    # In the first row, both the metadata and the submission need to be migrated
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", "gallery-dl",
            "--table-name", "metadata",
            "--file-key-type", "SHA256E",
            "--value", "source=test",
            "--value", "id=1",
            "--file-bytes", "metadata1"],
        expected_output_contains="Inserted row"
    )
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", "gallery-dl",
            "--table-name", "submissions",
            "--file-key-type", "SHA256E",
            "--value", "source=test",
            "--value", "id=1",
            "--value", f"metadata_file_key={str(Sha256HSe.from_bytes(b"metadata1", "txt"))}",
            "--value", "part=1",
            "--file-bytes", "submission1"],
        expected_output_contains="Inserted row"
    )

    # In the second row, only the submission needs to be migrated.
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", "gallery-dl",
            "--table-name", "metadata",
            "--file-key-type", "SHA256_HSe",
            "--value", "source=test",
            "--value", "id=2",
            "--file-bytes", "metadata2"],
        expected_output_contains="Inserted row"
    )
    await run(
        args=[
            "dolt-annex", "dataset", "insert-record",
            "--dataset", "gallery-dl",
            "--table-name", "submissions",
            "--file-key-type", "SHA256E",
            "--value", "source=test",
            "--value", "id=2",
            "--value", f"metadata_file_key={str(Sha256HSe.from_bytes(b"metadata2", "txt"))}",
            "--value", "part=1",
            "--file-bytes", "submission2"],
        expected_output_contains="Inserted row"
    )

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

        assert len(list(metadata_table.get_rows())) == 2
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

