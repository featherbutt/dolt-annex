#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.datatypes.file_stream import bytes_file_source
from dolt_annex.util.test_util import EnvironmentForTest, run

@pytest.mark.asyncio
async def test_export(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""

    key, _ = await setup.local_repo.put_file(bytes_file_source(b"new file content", Sha256E, extension="txt"))
    await run(
        args=["dolt-annex", "filestore", "export-file", str(key)],
        expected_output_contains="new file content"
    )

@pytest.mark.asyncio
async def test_filekey_from_stdin(tmp_path, setup: EnvironmentForTest):
    """Run and validate pushing content files to a remote"""

    key, _ = await setup.local_repo.put_file(bytes_file_source(b"new file content", Sha256E, extension="txt"))
    await run(
        args=["dolt-annex", "filestore", "export-file"],
        stdin=str(key) + "\n",
        expected_output_contains="new file content"
    )
