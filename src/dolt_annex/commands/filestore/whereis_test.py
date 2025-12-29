#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib

import pytest

from dolt_annex.file_keys.sha256e import Sha256e

from dolt_annex.test_util import run
# fixtures need to imported into the test namespace,
# even though they are not used directly here.
from dolt_annex.test_util.fixtures import *

local_repo_key = Sha256e.from_bytes(b"only in local repo", "txt")
both_repos_key = Sha256e.from_bytes(b"in both repos", "txt")

@pytest.fixture
def temp_dir(tmp_path):
    with contextlib.chdir(tmp_path):
        yield
     
@pytest_asyncio.fixture
async def whereis_setup(temp_dir, setup: TestSetup, scope="module"):
    """Run and validate pushing content files to a remote"""
    await run(
        args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "only in local repo"],
        expected_output="Inserted row"
    )

    await run(
        args=["dolt-annex", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "in both repos"],
        expected_output="Inserted row"
    )

    await run(
        args=["dolt-annex", "insert-record", "--repo", "test_remote", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "in both repos"],
        expected_output="Inserted row"
    )


@pytest.mark.asyncio
async def test1(temp_dir, whereis_setup):
        # Without --repo flag

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(local_repo_key).decode('utf-8')],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(both_repos_key).decode('utf-8')],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}, {{"name": "test_remote", "uuid": "{str(remote_uuid)}"}}]'
        )
        
        await run( 
            args=["dolt-annex", "whereis", "--file-key", "nonexistentkey"],
            expected_output='[]'
        )

        # With --repo flag

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(local_repo_key).decode('utf-8'), "--repo", "__local__"],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(both_repos_key).decode('utf-8'), "--repo", "__local__"],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(local_repo_key).decode('utf-8'), "--repo", "test_remote"],
            expected_output='[]'
        )

        await run(
            args=["dolt-annex", "whereis", "--file-key", bytes(both_repos_key).decode('utf-8'), "--repo", "test_remote"],
            expected_output=f'[{{"name": "test_remote", "uuid": "{str(remote_uuid)}"}}]'
        )