#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Helper functions that gallery-dl postprocessors can use to format data for dolt-annex.
"""

import hashlib
import json
import pathlib
import sys
from typing import Dict
from typing_extensions import Any, Optional

import fs.osfs
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.replicated_db.interface import TableFilter
from dolt_annex.replicated_db.dolt import FileTable, RepoDataset
from gallery_dl.util import json_default

from dolt_annex.datatypes import TableRow
from dolt_annex.datatypes.file_io import Path
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.file_keys import Sha256E

from dolt_annex.gallery_dl_plugin import _gallery_dl_context

from .sources import GalleryDLSource, get_source

def gallery_dl_post(metadata: dict):
    """The entrypoint for 'post' postprocessor hooks (run at the start of a batch of related downloads)"""
    context = _gallery_dl_context.get()
    if context.abort_flag:
        sys.exit(1)
        
    category = metadata["category"]
    subcategory = metadata["subcategory"]
    source = get_source(category, subcategory)

    source.format_post_metadata(metadata)

    context = _gallery_dl_context.get()
    repo_dataset: RepoDataset = context.repo_dataset


    # remove subcategory
    context.run(insert_metadata(metadata, source, repo_dataset, context.repo))
    context.post_metadata_files_processed += 1

def insert_metadata(metadata: Dict[str, Any], source: GalleryDLSource, repo_dataset: RepoDataset, repo: Repo):
    public_metadata = { k: v for k, v in metadata.items() if not source.exclude_field(k) }

    metadata_bytes = json.dumps(    
        public_metadata,
        ensure_ascii=False,
        sort_keys=True,
        indent=4,
        default=json_default).encode('utf-8') + b'\n'
    size = len(metadata_bytes)
    sha256 = hashlib.sha256(metadata_bytes).hexdigest()

    file_key = Sha256E.make(size, sha256, "json")
    metadata["_metadata_file_key"] = file_key

    table_row = TableRow({"source": source.source_name, "id": metadata["_id"], "file_key": file_key})

    table = repo_dataset.get_table("metadata")
    # return matadata file key on insertion
    cas = ContentAddressableStorage(repo.filestore, repo.key_format, repo.alternate_key_formats)

    return import_bytes(cas, table, table_row, metadata_bytes, "json", Sha256E)

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
    context = _gallery_dl_context.get()
    repo_dataset = context.repo_dataset
    repo = context.repo

    submissions_table = repo_dataset.get_table("submissions")
    page_number = source.page_number(metadata)
    metadata["_page_number"] = page_number
    filters = [
        TableFilter("source", source.source_name),
        TableFilter("id", metadata["_id"]),
        TableFilter("metadata_file_key", metadata["_metadata_file_key"]),
        TableFilter("part", page_number),
    ]
    if submissions_table.has_row(filters):
        # We already have this file, skip it.
        metadata["_skip"] = 1

    # Alternatively, if the source provides a hash in the metadata, we can check to see
    # Whether a file with that hash already exists in the filestore. If it does, we
    # make a task to insert a record into the table, and then skip.

    async def get_files_coro():
        for key_prefix in source.keys_from_metadata(metadata):
            async for key, _ in repo.filestore.get_files(bytes(key_prefix)):
                await submissions_table.insert(TableRow({
                    "source": source.source_name,
                    "id": metadata["_id"],
                    "metadata_file_key": metadata["_metadata_file_key"],
                    "part": page_number,
                    "submission_file_key": key
                }))
                metadata["_skip"] = 1
                return

    context.run(get_files_coro())
    return

def gallery_dl_after(metadata: dict[str, Any]):
    """The entrypoint for 'after' postprocessor hooks (run after downloading the file)"""
    category = metadata["category"]
    subcategory = metadata["subcategory"]
    source = get_source(category, subcategory)
    gallery_dl_import(source, metadata)

def gallery_dl_import(source: GalleryDLSource, metadata: dict):
    """Import the submission file and its metadata into the dolt-annex dataset."""

    context = _gallery_dl_context.get()
    repo_dataset = context.repo_dataset
    repo = context.repo

    submissions_table = repo_dataset.get_table("submissions")
    metadata_table = repo_dataset.get_table("metadata")

    temp_path = pathlib.Path(metadata["_path_metadata"].realpath)
    file_system = fs.osfs.OSFS(temp_path.parent.as_posix())
    temp_path = Path(file_system, temp_path.name)

    submission_table_key = TableRow({
        "source": source.source_name,
        "id": metadata["_id"],
        "metadata_file_key": metadata["_metadata_file_key"],
        "part": source.page_number(metadata)
    })
    cas = ContentAddressableStorage(repo.filestore, repo.key_format, repo.alternate_key_formats)
    context.run(import_file(cas, submissions_table, submission_table_key, temp_path, metadata["extension"], metadata["sha256"]))
    context.submission_files_processed += 1
    for metadata_key in source.file_metadata(metadata):
        context.run(import_file(cas, metadata_table, metadata_key, temp_path.parent / (temp_path.name + ".json"), "json"))
        context.submission_metadata_files_processed += 1

async def import_file(cas: ContentAddressableStorage, file_table: FileTable, table_key: TableRow, from_path: Path, extension: str, sha256: Optional[str] = None):
    """Import a file into the dolt-annex dataset, and add a corresponding row to given table with the given table key."""
    if not sha256:
        sha256 = from_path.hexdigest("sha256")
    size = from_path.stat().size
    assert size is not None

    file_key = Sha256E.make(size, sha256, extension)
    table_key["submission_file_key"] = str(file_key)

    await cas.put_file(from_path, file_key)
    await maybe_await(file_table.insert(table_key))

async def import_bytes(cas: ContentAddressableStorage, file_table: FileTable, table_key: TableRow, file_bytes: bytes, extension: str, file_key_type: type[FileKey]):
    """Import a file into the dolt-annex dataset, and add a corresponding row to given table with the given table key."""
    file_key = file_key_type.from_bytes(file_bytes, extension=extension)

    result = await cas.put_file_bytes(file_bytes, file_key)
    await result.wait_for_complete()
        
    await maybe_await(file_table.insert(table_key))