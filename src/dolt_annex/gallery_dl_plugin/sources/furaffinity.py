#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import override

from .base import GalleryDLSource

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
