#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any, Iterable

from typing_extensions import override

from dolt_annex.file_keys.base import FileKey, FileKeyPrefix
from dolt_annex.file_keys.base import MD5HSe

from .base import FileMetadata, GalleryDLSource, PathSelector, PostMetadata, mutate_remove_fields

class Gelbooru(GalleryDLSource, source_name = "gelbooru.com"):
    """Support for gelbooru.com"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["tag"]
    
    @override
    def fields_to_remove(self) -> PathSelector:
        return [
            "score",
            "search_tags",
            "has_comments",
            "has_notes",
        ]

    @override
    def num_pages(self, metadata: PostMetadata) -> int:
        return 1
    
    @override
    def keys_from_metadata(self, metadata: FileMetadata) -> Iterable[FileKey | FileKeyPrefix]:
        md5 = metadata["md5"]
        yield bytes(f"{MD5HSe.prefix}-{md5}", encoding="utf-8")

    @override
    def file_url_from_metadata(self, metadata: FileMetadata) -> str:
        return metadata["file_url"]

    @override
    def format_post_metadata(self, metadata: PostMetadata | FileMetadata):
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)
        if isinstance(metadata["tags"], str):
            metadata["tags"] = metadata["tags"].split(" ")
            
    format_file_metadata = format_post_metadata