#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, override

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
    def updated_date(self, metadata: dict[str, Any]) -> Any:
        return metadata.get("updated_at") or metadata["created_at"]
