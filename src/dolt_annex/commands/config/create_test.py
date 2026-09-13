#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import pytest

from dolt_annex.datatypes.collection import Collection
from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.filestore.archivefs import ArchiveFSModel
from dolt_annex.filestore.base import FileStoreModel
from dolt_annex.filestore.sqlite import SQLiteModel
from dolt_annex.test_util import run


@pytest.mark.asyncio
@pytest.mark.parametrize("create_type,create_class,name,create_json", [
    (
        "filestore",
        FileStoreModel,
        "foo_filestore",
        {
            "type": "annexfs",
            "root": ".",
        }
    ),
    (
        "repo",
        RepoModel,
        "foo_remote",
        {
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
            "filestore": {"type": "annexfs", "root": "."},
            "key_format": "SHA256E"
        }
    ),
    (
        "collection",
        Collection,
        "favorites",
        {
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
        }
    ),
    (
        "dataset",
        DatasetSchema,
        "foo_dataset",
        {
            "tables": [
                {
                    "name": "test_table",
                                "file_column": "file_key",
                                "key_columns": ["path"]
                }
            ],
            "empty_table_ref": "main"
        }
    ),
])
async def test_create(tmp_path, setup, create_class: type[Loadable], create_type, name, create_json):
    # Use new Loadable context to unload created remote so we can test reloading it.
    with Loadable.context():
        await run(
            args=["dolt-annex", "create", create_type, name, json.dumps(create_json)],
        )
    # Check that remote has been unloaded
    assert name not in create_class.cache.get()
    test_remote = create_class.must_load(name)
    assert name in create_class.cache.get()
    assert test_remote == create_class(name=name, **create_json)

@pytest.mark.asyncio
async def test_create_named_filestore(tmp_path, setup):
    secondary_json = {
        "type": "sqlite",
        "root": str(tmp_path / "secondary"),
    }
    archive_json = {
        "type": "archivefs",
        "root": str(tmp_path / "archive"),
        "secondary": "secondary",
    }
    await run(
        args=["dolt-annex", "create", "filestore", "secondary", json.dumps(secondary_json)],
    )
    await run(
        args=["dolt-annex", "create", "filestore", "archive", json.dumps(archive_json)],
    )
    archive_filestore = FileStoreModel.must_load("archive")
    assert isinstance(archive_filestore, ArchiveFSModel)
    assert isinstance(archive_filestore.secondary, SQLiteModel)
    assert archive_filestore.secondary.name == "secondary"
    assert archive_filestore.secondary.root == tmp_path / "secondary"

    dumped_filestore = archive_filestore.model_dump()
    assert dumped_filestore["secondary"] == "secondary"


@pytest.mark.asyncio
async def test_create_invalid_type(tmp_path, setup):
    await run(
        args=["dolt-annex", "create", "invalid", "name", "{}"],
        expected_error_code=1,
        expected_output_contains="Unknown command: create invalid. Accepted values are: repo, dataset",
    )
