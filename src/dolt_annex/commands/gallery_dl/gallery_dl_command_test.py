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
        args=["dolt-annex", "create", "collection", "favorites", '{"uuid": "123e4567-e89b-12d3-a456-426614174000"}'],
    )
    await run(
        args=["dolt-annex", "create", "collection", "empty", '{"uuid": "123e4567-e89b-12d3-a456-426614174001"}'],
    )
    await run(
        args=["dolt-annex", "init"],
    )
    await run(
        args=["dolt-annex", "gallery-dl", "https://e621.net/posts/3165771", "--collection", "favorites"],
    )
    await run(
        args=["dolt-annex", "dataset", "read-table", "--dataset", "gallery-dl", "--table-name", "submissions"],
        expected_output_contains='{"source": "e621.net", "id": 3165771, "metadata_file_key": "SHA256E-s3677--d43ff1b9d22eb56dc345a415f2c6f7de6afe4a39f8a3c1f97686f5ff5512bd03.json", "part": 1, "submission_file_key": "MD5_HSe-e08ee9f696b0c992be729298d0d6a58b--s4369998.png"}'
    )
    await run(
        args=["dolt-annex", "dataset", "read-table", "--dataset", "gallery-dl", "--table-name", "submissions", "--collection", "favorites"],
        expected_output_contains='{"source": "e621.net", "id": 3165771, "metadata_file_key": "SHA256E-s3677--d43ff1b9d22eb56dc345a415f2c6f7de6afe4a39f8a3c1f97686f5ff5512bd03.json", "part": 1, "submission_file_key": "MD5_HSe-e08ee9f696b0c992be729298d0d6a58b--s4369998.png"}'
    )
    await run(
        args=["dolt-annex", "dataset", "read-table", "--dataset", "gallery-dl", "--table-name", "submissions", "--collection", "empty"],
        expected_output_equals=''
    )
    await run(
        args=["dolt-annex", "filestore", "verify", "--repo", "__local__", "MD5_HSe-e08ee9f696b0c992be729298d0d6a58b--s4369998.png"],
    )