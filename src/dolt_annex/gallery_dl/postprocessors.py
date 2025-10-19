#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Helper functions that gallery-dl postprocessors can use to format data for dolt-annex.
"""

import hashlib
import json
from pathlib import Path
from typing_extensions import Any, Optional

from gallery_dl.util import json_default

from dolt_annex import config
from dolt_annex.datatypes.common import TableRow, AnnexKey
from dolt_annex.datatypes.remote import Repo
from dolt_annex.table import Dataset, FileTable
from dolt_annex.gallery_dl import dataset_context
from dolt_annex.filestore import get_key_path

from .sources import GalleryDLSource, get_source

def gallery_dl_post(metadata: dict):
    """The entrypoint for 'post' postprocessor hooks (run at the start of a batch of related downloads)"""
    category = metadata["category"]
    subcategory = metadata["subcategory"]
    source = get_source(category, subcategory)

    source.format_post_metadata(metadata)

    for table_row in source.post_metadata(metadata):
        dataset: Dataset = dataset_context.get()
        local_repo = dataset.dataset_source.repo
        # remove subcategory
        public_metadata = { k: v for k, v in metadata.items() if not source.exclude_field(k) }
        metadata_bytes = json.dumps(
            public_metadata,
            ensure_ascii=False,
            sort_keys=True,
            indent=4,
            default=json_default).encode('utf-8') + b'\n'
        table: FileTable = dataset.get_table("metadata")
        import_bytes(local_repo, table, table_row, metadata_bytes, "json")

def gallery_dl_prepare(metadata: dict[str, Any]):
    """The entrypoint for 'prepare' postprocessor hooks (run before downloading the file)"""
    category = metadata["category"]
    subcategory = metadata["subcategory"]
    source = get_source(category, subcategory)

    source.format_file_metadata(metadata)
    check_skip(source, metadata)

def check_skip(source: GalleryDLSource, metadata: dict[str, Any]):
    """Check whether we should skip downloading this file."""
    # First, check whether we already have the file in the annex.
    # TODO: We may want to skip if any known remote has a copy, not just the local remote.
    dataset = dataset_context.get()
    file_table = dataset.get_table("submissions")
    uuid = dataset.dataset_source.repo.uuid
    key = source.table_key(metadata)
    if file_table.has_row(uuid, key):
        # We already have this file, skip it.
        metadata["_skip"] = 1

def gallery_dl_after(metadata: dict[str, Any]):
    """The entrypoint for 'after' postprocessor hooks (run after downloading the file)"""
    category = metadata["category"]
    subcategory = metadata["subcategory"]
    source = get_source(category, subcategory)
    gallery_dl_import(source, metadata)

def gallery_dl_import(source: GalleryDLSource, metadata: dict):
    """Import the submission file and its metadata into the dolt-annex dataset."""
    
    dataset: Dataset = dataset_context.get()
    local_repo = dataset.dataset_source.repo

    submissions_table = dataset.get_table("submissions")
    metadata_table = dataset.get_table("metadata")

    temp_path = Path(metadata["_path_metadata"].realpath)

    import_file(local_repo, submissions_table, source.table_key(metadata), temp_path, metadata["extension"], metadata["sha256"])
    for metadata_key in source.file_metadata(metadata):
        import_file(local_repo, metadata_table, metadata_key, temp_path.parent / (temp_path.name + ".json"), "json")


def import_file(local_remote: Repo, file_table: FileTable, table_key: TableRow, from_path: Path, extension: str, sha256: Optional[str] = None):
    """Import a file into the dolt-annex dataset, and add a corresponding row to given table with the given table key."""
    if not sha256:
        sha256 = hashlib.sha256(from_path.read_bytes()).hexdigest()
    size = from_path.stat().st_size

    file_key = make_file_key(size, sha256, extension)

    output_path = local_remote.files_dir() / get_key_path(file_key)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    from_path.rename(output_path)
    file_table.insert_file_source(table_key, file_key, config.get_config().local_uuid)

def import_bytes(local_remote: Repo, file_table: FileTable, table_key: TableRow, file_bytes: bytes, extension: str, sha256: Optional[str] = None):
    """Import a file into the dolt-annex dataset, and add a corresponding row to given table with the given table key."""
    if not sha256:
        sha256 = hashlib.sha256(file_bytes).hexdigest()
    size = len(file_bytes)

    file_key = make_file_key(size, sha256, extension)

    output_path = local_remote.files_dir() / get_key_path(file_key)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "wb") as f:
        f.write(file_bytes)

    file_table.insert_file_source(table_key, file_key, config.get_config().local_uuid)

def make_file_key(size, sha256, extension) -> AnnexKey:
    """Computes the file key for a file with the given size, sha256 hash, and extension."""
    return AnnexKey(f"SHA256E-s{size}--{sha256}.{extension}")
