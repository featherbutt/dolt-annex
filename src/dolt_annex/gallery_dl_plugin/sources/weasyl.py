#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, override

from .base import GalleryDLSource

class Weasyl(GalleryDLSource, source_name = "weasyl.com"):
    """Support for weasyl.com"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["submission"]
    
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "owner_media",
            "comments",
            "favorites",
            "views",
            "favorited"
        ]
    
    @override
    def format_post_metadata(self, metadata: dict[str, Any]):
        super().format_post_metadata(metadata)

        metadata["id"] = metadata.pop("submitid")
