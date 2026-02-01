#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pytest
import pytest_asyncio

from dolt_annex.file_keys.sha256e import Sha256e

from dolt_annex.test_util import run, EnvironmentForTest

local_repo_key = Sha256e.from_bytes(b"only in local repo", "txt")
both_repos_key = Sha256e.from_bytes(b"in both repos", "txt")
     
@pytest_asyncio.fixture
async def whereis_setup(setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""
    await run(
        args=["dolt-annex", "dataset", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "only in local repo"],
        expected_output_contains="Inserted row"
    )

    await run(
        args=["dolt-annex", "dataset", "insert-record", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "in both repos"],
        expected_output_contains="Inserted row"
    )

    await run(
        args=["dolt-annex", "dataset", "insert-record", "--repo", "test_remote", "--dataset", "test", "--table-name", "test_table", "--key-columns", "test_key", "--file-bytes", "in both repos"],
        expected_output_contains="Inserted row"
    )
    return setup


@pytest.mark.asyncio
async def test_basic(whereis_setup: EnvironmentForTest):
        # Without --repo flag
        local_uuid = whereis_setup.local_repo.uuid
        remote_uuid = whereis_setup.remote_repo.uuid

        await run(
            args=["dolt-annex", "filestore", "whereis", bytes(local_repo_key).decode('utf-8')],
            expected_output_contains=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", bytes(both_repos_key).decode('utf-8')],
            expected_output_contains=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}, {{"name": "test_remote", "uuid": "{str(remote_uuid)}"}}]'
        )
        
        await run( 
            args=["dolt-annex", "filestore", "whereis", "nonexistentkey"],
            expected_output_contains='[]'
        )

        # With --repo flag

        await run(
            args=["dolt-annex", "filestore", "whereis", bytes(local_repo_key).decode('utf-8'), "--repo", "__local__"],
            expected_output_contains=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", bytes(both_repos_key).decode('utf-8'), "--repo", "__local__"],
            expected_output_contains=f'[{{"name": "__local__", "uuid": "{str(local_uuid)}"}}]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", bytes(local_repo_key).decode('utf-8'), "--repo", "test_remote"],
            expected_output_contains='[]'
        )

        await run(
            args=["dolt-annex", "filestore", "whereis", bytes(both_repos_key).decode('utf-8'), "--repo", "test_remote"],
            expected_output_contains=f'[{{"name": "test_remote", "uuid": "{str(remote_uuid)}"}}]'
        )