#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pytest
import pytest_asyncio

from dolt_annex.file_keys.sha256e import Sha256e

from dolt_annex.test_util import run, EnvironmentForTest, local_uuid, remote_uuid

local_repo_key = Sha256e.from_bytes(b"only in local repo", "txt")
both_repos_key = Sha256e.from_bytes(b"in both repos", "txt")
     
@pytest_asyncio.fixture
async def whereis_setup(temp_dir, setup: EnvironmentForTest):
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
async def test_basic(temp_dir, whereis_setup):
        # Without --repo flag

        await run(
            args=["dolt-annex", "filestore", "whereis", "--file-key", bytes(local_repo_key).decode('utf-8')],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", "--file-key", bytes(both_repos_key).decode('utf-8')],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}, {{"name": "test_remote", "uuid": "{str(remote_uuid)}"}}]'
        )
        
        await run( 
            args=["dolt-annex", "filestore", "whereis", "--file-key", "nonexistentkey"],
            expected_output='[]'
        )

        # With --repo flag

        await run(
            args=["dolt-annex", "filestore", "whereis", "--file-key", bytes(local_repo_key).decode('utf-8'), "--repo", "__local__"],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", "--file-key", bytes(both_repos_key).decode('utf-8'), "--repo", "__local__"],
            expected_output=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", "--file-key", bytes(local_repo_key).decode('utf-8'), "--repo", "test_remote"],
            expected_output='[]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", "--file-key", bytes(both_repos_key).decode('utf-8'), "--repo", "test_remote"],
            expected_output=f'[{{"name": "test_remote", "uuid": "{str(remote_uuid)}"}}]'
        )