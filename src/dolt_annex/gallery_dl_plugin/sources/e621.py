#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Iterable

from typing_extensions import Any, override

from dolt_annex.file_keys.base import FileKey, FileKeyPrefix
from dolt_annex.file_keys.size_hash_extension import MD5e

from .base import GalleryDLSource, mutate_remove_fields

class E621(GalleryDLSource, source_name = "e621.net"):
    """Support for e621.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["post", "tag"]
    
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "score",
            "fav_count",
            "comment_count",
            "is_favorited",
            "flags",
            "search_tags",
            "vote",
        ]

    @override
    def keys_from_metadata(self, metadata: dict[str, Any]) -> Iterable[FileKey | FileKeyPrefix]:
        file_info = metadata["file"]
        md5 = file_info["md5"]
        size = file_info["size"]
        ext = file_info["ext"]
        yield MD5e.make(size, md5, ext)

    def format_post_metadata(self, metadata: dict[str, Any]):
        """
        gallery-dl changed how tags are formatted. We continue to represent tags using the previous
        formatting for consistency.
        """
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)
        old_tags = metadata["tags"]
        if isinstance(old_tags, dict):
            tag_types = ["artist", "character", "contributor", "copyright", "general", "invalid", "lore", "meta", "species"]
            all_tags = []
            for tag_type in tag_types:
                metadata[f"tags_{tag_type}"] = old_tags[tag_type]
                all_tags.extend(old_tags[tag_type])
            
            #tags = { tag_type: metadata.pop(f"tags_{tag_type}", []) for tag_type in tag_types }
            metadata["tags"] = all_tags
        metadata["tags"].sort()
            
    format_file_metadata = format_post_metadata