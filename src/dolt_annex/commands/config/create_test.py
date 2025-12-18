#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import pytest

from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.test_util import run, setup


@pytest.mark.asyncio
@pytest.mark.parametrize("create_type,create_class,name,create_json", [
    (
        "repo",
        Repo,
        "foo_remote",
        {
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
            "filestore": {"type": "annexfs", "root": "."},
            "key_format": "Sha256e"
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
async def test_create_remote(tmp_path, create_class: type[Loadable], create_type, name, create_json):
    async with setup(tmp_path):
        # Use new Loadable context to unload created remote so we can test reloading it.
        with Loadable.context():
            await run(
                args=["dolt-annex", "create", create_type, name, json.dumps(create_json)],
            )
        # Check that remote has been unloaded
        assert name not in create_class.cache.get()
        test_remote = create_class.must_load(name)
        assert name in create_class.cache.get()
        assert test_remote.model_dump(mode='json') == {"name": name, **create_json}

@pytest.mark.asyncio
async def test_create_invalid_type(tmp_path):
    async with setup(tmp_path):
        await run(
            args=["dolt-annex", "create", "invalid", "name", "{}"],
            expected_error_code=1,
            expected_output="Unknown command: create invalid. Accepted values are: repo, dataset",
        )
