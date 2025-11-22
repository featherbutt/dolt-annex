#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import contextlib

from dolt_annex.test_util import run, setup

def test_pull_local(tmp_path):
    asyncio.run(setup(tmp_path))
    do_test_pull(tmp_path)

def do_test_pull(tmp_path):
    """Run and validate pulling content files from a remote"""

    with contextlib.chdir(tmp_path):
        run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key1",
                  "--file-bytes", "file_content_1",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )
        run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key2",
                  "--file-bytes", "file_content_2",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )

        run(
            args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
            expected_output="Pulled 2 files from remote test_remote"
        )

        # Pulling again should result in no files being pulled
        run(
            args=["dolt-annex", "pull","--dataset", "test", "--remote", "test_remote"],
            expected_output="Pulled 0 files from remote test_remote"
        )

        # But if we add more files, it should push them
        run(
            args=["dolt-annex", "insert-record",
                  "--dataset", "test",
                  "--table-name", "test_table",
                  "--key-columns", "test_key3",
                  "--file-bytes", "file_content_3",
                  "--remote", "test_remote"],
            expected_output="Inserted row"
        )
        run(
            args=["dolt-annex", "pull", "--dataset", "test", "--remote", "test_remote"],
            expected_output="Pulled 1 files from remote test_remote"
        )
