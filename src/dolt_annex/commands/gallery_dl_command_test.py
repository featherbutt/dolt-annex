#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib

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
        args=["dolt-annex", "gallery-dl", "https://www.furaffinity.net/view/63142315/"],
    )
    await run(
        args=["dolt-annex", "read-table", "--dataset", "gallery-dl", "--table-name", "submissions"],
        expected_output="SHA256E-s3204233--28c9485eec3f2e33fa7c0f3c7a5ae62f94e939f3a494e4c5e7dfd16d8c8776c7.png, furaffinity.net, 63142315"
    )
        
