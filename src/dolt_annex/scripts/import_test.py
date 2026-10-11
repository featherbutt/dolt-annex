#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.gallery_dl_plugin import make_default_schema
from dolt_annex.util.test_util import run

@pytest.mark.asyncio
async def test_import(temp_dir, setup):
    """
    Test the basic functionality of the import subcommand.
    """

    # A file path matching the pattern expected by dolt_annex.importers.gallerydl.GalleryDL
    file_path = temp_dir / "import_dir" / "posts" / "0" / "0" / "1_None.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open('wb') as f:
        f.write(b"test image data")

    expected_file_key = str(Sha256E.from_bytes(b"test image data", extension="json"))

    await run(
        args=["dolt-annex", "init"],
    )

    dataset_schema = make_default_schema("gallery-dl")
    
    await run(
        args=[
            "dolt-annex", "script", "import", "--",
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
        expected_output_equals=f'{{"source": "test.com/posts", "id": 1, "file_key": "{expected_file_key}"}}\n'
    )
    
    await run(
        args=[
            "dolt-annex", "filestore", "export-file",
            expected_file_key,
        ],
        expected_output_equals="test image data"
    )
    
    
