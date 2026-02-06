#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.test_util import EnvironmentForTest, run

@pytest.mark.asyncio
async def test_export(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""

    key = Sha256E.from_bytes(b"new file content", "txt")
    result = await setup.local_file_store.put_file_bytes(b"new file content", key)
    await result.wait_for_complete()
    await run(
        args=["dolt-annex", "filestore", "export-file", "--file-key", str(key)],
        expected_output_contains="new file content"
    )

@pytest.mark.asyncio
async def test_filekey_from_stdin(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""

    key = Sha256E.from_bytes(b"new file content", "txt")
    result = await setup.local_file_store.put_file_bytes(b"new file content", key)
    await result.wait_for_complete()
    await run(
        args=["dolt-annex", "filestore", "export-file"],
        stdin=str(key) + "\n",
        expected_output_contains="new file content"
    )
