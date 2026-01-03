#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module contains pytest fixtures for setting up tests.

Since fixtures must exist in the same namespace as the tests that use them,
despite some of them only being transitively referenced, it's okay to import
this module with:

from dolt_annex.test_util import *
"""

import pathlib
import shutil
import contextlib

from plumbum import local # type: ignore
import pytest
import pytest_asyncio

from dolt_annex.data import data_dir
from dolt_annex.datatypes.loader import Loadable
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.test_util import create_test_filestore, local_uuid, remote_uuid, test_config, EnvironmentForTest

@pytest.fixture
def temp_dir(tmp_path):
    with contextlib.chdir(tmp_path):
        yield

@pytest.fixture()
def dolt(temp_dir, tmp_path):
    dolt_dir = pathlib.Path(tmp_path / "dolt")
    dolt_dir.mkdir()
    dolt = local.cmd.dolt.with_cwd(dolt_dir)
    shutil.copytree(data_dir / "dolt_base" / ".dolt", dolt_dir / ".dolt")
    yield dolt

@pytest.fixture
def init_dolt(dolt):
    dolt("checkout", "-b", "test_dataset")
    dolt("sql", "-q", "CREATE TABLE test_table(path varchar(100) primary key, annex_key varchar(100));")
    dolt("add", ".")
    dolt("commit", "-m", "Initial commit")
    yield dolt


@pytest_asyncio.fixture 
async def setup(tmp_path: pathlib.Path, init_dolt):

    # Use Loadable.context to ensure that registered types will be reset at the end of the test.
    with Loadable.context():
        local_filestore = await create_test_filestore("__local__", local_uuid, [])
        remote_filestore = await create_test_filestore("test_remote", remote_uuid, [])

        Repo(
            name="test_remote",
            uuid=remote_uuid,
            filestore=remote_filestore.file_store,
            key_format=Sha256e
        )

        with (tmp_path / "config.json").open("w") as f:
            f.write(test_config.model_dump_json())

        async with (
            local_filestore.open(test_config),
            remote_filestore.open(test_config)
        ):
            with contextlib.chdir(tmp_path):
                yield EnvironmentForTest(
                    local_file_store=local_filestore,
                    remote_file_store=remote_filestore,
                )