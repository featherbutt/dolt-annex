#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any, Iterable

from typing_extensions import override

from dolt_annex.file_keys.base import FileKey, FileKeyPrefix
from dolt_annex.file_keys.hash_size_extension import MD5HSe

from .base import GalleryDLSource, PathSelector, mutate_remove_fields

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
    def keys_from_metadata(self, metadata: dict[str, Any]) -> Iterable[FileKey | FileKeyPrefix]:
        md5 = metadata["md5"]
        yield bytes(f"{MD5HSe.prefix}-{md5}", encoding="utf-8")

    def format_post_metadata(self, metadata: dict[str, Any]):
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)
        if isinstance(metadata["tags"], str):
            metadata["tags"] = metadata["tags"].split(" ")
            
    format_file_metadata = format_post_metadata