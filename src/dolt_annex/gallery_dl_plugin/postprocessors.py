#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Helper functions that gallery-dl postprocessors can use to format data for dolt-annex.
"""

import json
import sys
from typing import Dict
import gallery_dl
from typing_extensions import Any

from gallery_dl.util import json_default

from dolt_annex.datatypes.errors import DoltAnnexApplicationError
from dolt_annex.datatypes.file_io import Pipe, SyncPipeWriter
from dolt_annex.file_keys.base import FileKey, Sha256E
from dolt_annex.filestore.base import FileStore
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.datatypes.file_stream import KeyedFileStream, bytes_file_source, file_source
from dolt_annex.gallery_dl_plugin.context import GalleryDLContext
from dolt_annex.gallery_dl_plugin.sources.base import FileMetadata, PostMetadata
from dolt_annex.replicated_db.interface import ReplicatedDataset, ReplicatedTable, TableFilter


from dolt_annex.datatypes import TableRow

from dolt_annex.gallery_dl_plugin import _gallery_dl_context

from .sources import GalleryDLSource, get_source

def gallery_dl_post(metadata: PostMetadata):
    """The entrypoint for 'post' postprocessor hooks (run at the start of a batch of related downloads)"""
    context = _gallery_dl_context.get()
    if context.abort_flag:
        sys.exit(1)

    try:
        category = metadata["category"]
        subcategory = metadata["subcategory"]
        source = get_source(category, subcategory)

        source.format_post_metadata(metadata)

        dataset: ReplicatedDataset = context.dataset

        context.run(insert_metadata(metadata, source, dataset, context.repo, context.collections_metadata_tables))
        context.post_metadata_files_processed += 1
    except DoltAnnexApplicationError as e:
        context.exception.set_exception(e)
        raise gallery_dl.exception.TerminateExtraction()


def serialize_metadata(metadata: Dict[str, Any], source: GalleryDLSource, repo: Repo):
    source.format_post_metadata(metadata)
    public_metadata = { k: v for k, v in metadata.items() if not source.exclude_field(k) }

    return json.dumps(    
        public_metadata,
        ensure_ascii=False,
        sort_keys=True,
        indent=4,
        default=json_default).encode('utf-8') + b'\n'


async def insert_metadata(metadata: Dict[str, Any], source: GalleryDLSource, dataset: ReplicatedDataset, repo: Repo, collections: GalleryDLContext.CollectionTables = GalleryDLContext.CollectionTables([])):
    metadata_bytes = serialize_metadata(metadata, source, repo)
    
    table_row = TableRow({"source": source.source_name, "id": metadata["_id"]})

    async with dataset.with_table("metadata") as table:
        file_key = await import_file(repo, table_row, bytes_file_source(metadata_bytes, Sha256E, extension="json"), table, collections)
    metadata["_metadata_file_key"] = file_key
    return file_key

def gallery_dl_prepare(metadata: FileMetadata):
    """The entrypoint for 'prepare' postprocessor hooks (run before downloading the file)"""
    context = _gallery_dl_context.get()
    try:
        category = metadata["category"]
        subcategory = metadata["subcategory"]
        source = get_source(category, subcategory)
        context.stdout.write(f"Downloading post {metadata['_id']}...")

        repo = context.repo
        dataset = context.dataset

        source.format_file_metadata(metadata)
        if check_skip(source, metadata):
            metadata["_skip"] = 1
            context.stdout.write("Skipped\n")
        else:
            insert_row = TableRow({
                "source": source.source_name,
                "id": metadata["_id"],
                "metadata_file_key": metadata["_metadata_file_key"],
                "part": source.page_number(metadata),
            })
            for key in source.keys_from_metadata(metadata):
                if isinstance(key, FileKey):
                    key_type = type(key)
                    break
            else:
                key_type = Sha256E
            def custom_open(self, mode):
                pipe = Pipe(context.event_loop)
                writer = SyncPipeWriter(pipe)
                async def task():
                    async with dataset.with_table("submissions") as submissions_table:
                        await import_file(
                            repo,
                            insert_row,
                            file_source(pipe.open(), key_type, extension=metadata["extension"]),
                            submissions_table,
                            context.collections_submission_tables
                        )
                context.enqueue_task(task())
                return writer
            from gallery_dl.path import PathFormat
            PathFormat.open = custom_open
    except DoltAnnexApplicationError as e:
        context.exception.set_exception(e)
        raise gallery_dl.exception.TerminateExtraction()

def check_skip(source: GalleryDLSource, metadata: FileMetadata) -> bool:
    """Check whether we should skip downloading this file."""
    # First, check whether we already have the file in the annex.

    context = _gallery_dl_context.get()

    # First, if the --skip-download flag is set, we skip all downloads.
    if _gallery_dl_context.get().config.skip_download:
        return True

    repo_dataset = context.repo_dataset
    repo = context.repo
    dataset = context.dataset

    submissions_table = repo_dataset.get_table("submissions")
    metadata_table = repo_dataset.get_table("metadata")
    page_number = source.page_number(metadata)
    metadata["_page_number"] = page_number
    filters = [
        TableFilter("source", source.source_name),
        TableFilter("id", metadata["_id"]),
        TableFilter("metadata_file_key", metadata["_metadata_file_key"]),
        TableFilter("part", page_number),
    ]
    if (row := submissions_table.get_row(filters=filters)):
        # We already have this file, skip it.
        context.run(context.collections_submission_tables.insert(row))
        return True

    # Alternatively, if the source provides a hash in the metadata, we can check to see
    # Whether a file with that hash already exists in the filestore. If it does, we
    # make a task to insert a record into the table, and then skip.

    async def get_files_coro() -> bool:
        has_keys_in_metadata = False
        for key_prefix in source.keys_from_metadata(metadata):
            has_keys_in_metadata = True
            try:
                async for data_source in repo.get_files(bytes(key_prefix)):
                    
                    row = TableRow({
                        "source": source.source_name,
                        "id": metadata["_id"],
                        "metadata_file_key": metadata["_metadata_file_key"],
                        "part": page_number,
                    })
                    async with dataset.with_table("submissions") as writable_submissions_table:
                        await import_file(
                            repo,
                            row,
                            data_source,
                            writable_submissions_table,
                            context.collections_submission_tables
                        )
                    return True
            except FileStore.GetFilesNotImplementedError:
                pass
        
        if not has_keys_in_metadata:
            # If the source doesn't contain file hashes, it might contain other information
            # That can tell us if the submission is unchanged from a previous version.
            for row in metadata_table.get_rows(filters=[
                TableFilter("source", source.source_name),
                TableFilter("id", metadata["_id"])
            ]):
                metadata_file_key = FileKey.must_parse(row["file_key"])
                metadata_bytes = await repo.file_store.get_file_bytes(metadata_file_key)
                existing_metadata = json.loads(metadata_bytes)
                if source.assume_same_file(metadata, existing_metadata, page_number):
                    submission_row = submissions_table.get_row(filters=[
                        TableFilter("source", source.source_name),
                        TableFilter("id", metadata["_id"]),
                        TableFilter("metadata_file_key", metadata_file_key),
                        TableFilter("part", page_number),
                    ])
                    if submission_row is not None:
                        row = TableRow({
                            "source": source.source_name,
                            "id": metadata["_id"],
                            "metadata_file_key": metadata["_metadata_file_key"],
                            "part": page_number,
                            "submission_file_key": submission_row["submission_file_key"],
                        })
                        await submissions_table.insert(row)
                        await context.collections_submission_tables.insert(row)
                        return True
        return False

    return context.run(get_files_coro())

def gallery_dl_after(metadata: dict[str, Any]):
    """The entrypoint for 'after' postprocessor hooks (run after downloading the file)"""
    context = _gallery_dl_context.get()
    try:
        category = metadata["category"]
        subcategory = metadata["subcategory"]
        source = get_source(category, subcategory)
        gallery_dl_import(source, metadata)
        context.stdout.write("Done\n")
    except DoltAnnexApplicationError as e:
        context.exception.set_exception(e)
        raise gallery_dl.exception.TerminateExtraction()

def gallery_dl_import(source: GalleryDLSource, metadata: dict):
    """Import the submission file and its metadata into the dolt-annex dataset."""

    context = _gallery_dl_context.get()
    repo_dataset = context.repo_dataset

    context.submission_files_processed += 1
    for metadata_table_key in source.file_metadata(metadata):
        # TODO: This doesn't currently work properly for sources that have metadata for each file.
        # But Itaku is the only such source and it's currently broken anyway.
        raise NotImplementedError("metadata on files is not currently supported")
        context.run(insert_metadata(metadata, source, repo_dataset, context.repo, context.collections_metadata_tables))

async def import_file(
    cas: ContentAddressableStorage,
    table_row: TableRow,
    data_source: KeyedFileStream,
    replicated_table: ReplicatedTable,
    collections: GalleryDLContext.CollectionTables,
) -> FileKey:
    file_key, filestore_ids = await cas.put_file(data_source)
    table_row[replicated_table.schema.file_column] = file_key
    for filestore_id in filestore_ids:
        async with replicated_table.with_repo(filestore_id) as table_replica:
            await table_replica.insert(table_row)
    await collections.insert(table_row)
    return file_key