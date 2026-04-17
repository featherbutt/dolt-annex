#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.test_util import run

@pytest.mark.asyncio
async def test_import(temp_dir, setup):
    """
    Test the basic functionality of the import subcommand.
    """

    # A file path matching the pattern expected by dolt_annex.importers.gallerydl.GalleryDL
    file_path = temp_dir / "import_dir" / "posts" / "0" / "0" / "1_None.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open('wb') as f:
        f.write(b"test image data")

    expected_file_key = str(Sha256E.from_bytes(b"test image data", extension="jpg"))

    await run(
        args=["dolt-annex", "init"],
    )
    # Calling gallery-dl will initialize the dataset, then fail because there are no URLs.
    # TODO: Run "dolt-annex config create" instead.
    await run(
        args=["dolt-annex", "gallery-dl"],
        expected_exception=SystemExit
    )
    await run(
        args=[
            "dolt-annex", "import",
            "--importer", "gallerydl.GalleryDL test.com",
            "--move",
            "--dataset",  "gallery-dl",
            "import_dir"
        ],
    )

    # Verify that the file was imported into the dataset
    await run(
        args=[
            "dolt-annex", "dataset", "read-table",
            "--dataset", "gallery-dl",
            "--table-name", "metadata",
            "--columns", "source",
            "--columns", "id",
            "--columns", "file_key"
        ],
        expected_output_equals=f'test.com/posts, 1, {expected_file_key}\n'
    )
    
    await run(
        args=[
            "dolt-annex", "filestore", "export-file",
            expected_file_key,
        ],
        expected_output_equals="test image data"
    )
    
    
