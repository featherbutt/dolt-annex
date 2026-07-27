#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.test_util import run

@pytest.mark.asyncio
async def test_gallery_dl(tmp_path, setup):
    """
    Test the basic functionality of the gallery-dl subcommand,
    downloading files using gallery-dl and inserting them into a dolt-annex dataset.
    """
    await run(
        args=["dolt-annex", "init"],
    )
    await run(
        args=["dolt-annex", "gallery-dl", "https://e621.net/posts/14"],
    )
    await run(
        args=["dolt-annex", "dataset", "read-table", "--dataset", "gallery-dl", "--table-name", "submissions"],
        expected_output_contains='{"source": "e621.net", "id": 14, "metadata_file_key": "SHA256E-s8476--57c502491c45f4c6fbd66cf150c120400f23a343e57c592269e0969c6ae8f337.json", "part": 1, "submission_file_key": "MD5_HSe-3e47080200fbde2d7d2ccf419343ab0a--s96998.jpg"}'
    )
