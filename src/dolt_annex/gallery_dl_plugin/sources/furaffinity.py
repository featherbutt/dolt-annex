#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any

from typing_extensions import override

from .base import GalleryDLSource, mutate_remove_fields

class Furaffinity(GalleryDLSource, source_name = "furaffinity.net"):
    """Support for furaffinity.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["post", "search", "favorite", "gallery"]
    
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "comments",
            "favorites",
            "views",
            "search",
            "user",
            "favorite_id",
        ]
    
    def format_post_metadata(self, metadata: dict[str, Any]):
        """
        A bug in a previous version of gallery-dl would accidentally include an erroneous "Keywords" tag.
        """
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)
        tags: list[str] = metadata["tags"]
        if "Keywords" in tags:
            tags.remove("Keywords")
        
            
