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
from dolt_annex.datatypes.file_stream import bytes_file_source, file_source
from dolt_annex.gallery_dl_plugin.sources.base import FileMetadata, PostMetadata
from dolt_annex.replicated_db.interface import TableFilter

from dolt_annex.datatypes import TableRow

from dolt_annex.gallery_dl_plugin import _gallery_dl_context
from dolt_annex.writer import ReplicaWriter

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

        context.run(insert_metadata(metadata, source, context.metadata_writer))
        context.post_metadata_files_processed += 1
    except DoltAnnexApplicationError as e:
        context.exception.set_exception(e)
        raise gallery_dl.exception.TerminateExtraction()


def serialize_metadata(metadata: Dict[str, Any], source: GalleryDLSource):
    source.format_post_metadata(metadata)
    public_metadata = { k: v for k, v in metadata.items() if not source.exclude_field(k) }

    return json.dumps(    
        public_metadata,
        ensure_ascii=False,
        sort_keys=True,
        indent=4,
        default=json_default).encode('utf-8') + b'\n'


async def insert_metadata(metadata: Dict[str, Any], source: GalleryDLSource, writer: ReplicaWriter):
    metadata_bytes = serialize_metadata(metadata, source)
    
    table_row = TableRow({"source": source.source_name, "id": metadata["_id"]})

    file_key = await writer.put_file(table_row, bytes_file_source(metadata_bytes, Sha256E, extension="json"))
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
                context.enqueue_task(
                    context.submission_writer.put_file(
                        insert_row,
                        file_source(pipe.open(), key_type, extension=metadata["extension"])
                    )
                )
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
    repo = context.repo
    submissions = context.submission_writer
    metadata_ = context.metadata_writer

    # First, if the --skip-download flag is set, we skip all downloads.
    if context.config.skip_download:
        return True

    page_number = source.page_number(metadata)
    metadata["_page_number"] = page_number
    filters = [
        TableFilter("source", source.source_name),
        TableFilter("id", metadata["_id"]),
        TableFilter("metadata_file_key", metadata["_metadata_file_key"]),
        TableFilter("part", page_number),
    ]
    if (row := submissions.table.get_row(filters=filters)):
        # We already have this file, skip it. (But add it to collections)
        context.run(submissions.collections.insert(row))
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
                        
                        await submissions.put_file(row, data_source)
                        return True
                except FileStore.GetFilesNotImplementedError:
                    pass
            
            if not has_keys_in_metadata:
                # If the source doesn't contain file hashes, it might contain other information
                # That can tell us if the submission is unchanged from a previous version.
                for row in metadata_.table.get_rows(filters=[
                    TableFilter("source", source.source_name),
                    TableFilter("id", metadata["_id"])
                ]):
                    metadata_file_key = FileKey.must_parse(row["file_key"])
                    metadata_bytes = await repo.file_store.get_file_bytes(metadata_file_key)
                    existing_metadata = json.loads(metadata_bytes)
                    if source.assume_same_file(metadata, existing_metadata, page_number):
                        submission_row = submissions.table.get_row(filters=[
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
                            await submissions.insert(row)
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

    context.submission_files_processed += 1
    for metadata_table_key in source.file_metadata(metadata):
        # TODO: This doesn't currently work properly for sources that have metadata for each file.
        # But Itaku is the only such source and it's currently broken anyway.
        raise NotImplementedError("metadata on files is not currently supported")
        context.run(insert_metadata(metadata, source, context.metadata_writer))
