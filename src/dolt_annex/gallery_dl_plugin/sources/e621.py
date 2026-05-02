#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Iterable

from typing_extensions import Any, override

from dolt_annex.file_keys.base import FileKey, FileKeyPrefix
from dolt_annex.file_keys.size_hash_extension import MD5e

from .base import GalleryDLSource

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
        ]

    @override
    def keys_from_metadata(self, metadata: dict[str, Any]) -> Iterable[FileKey | FileKeyPrefix]:
        file_info = metadata["file"]
        md5 = file_info["md5"]
        size = file_info["size"]
        ext = file_info["ext"]
        yield MD5e.make(size, md5, ext)

