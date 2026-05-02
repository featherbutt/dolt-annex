#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, override

from .base import GalleryDLSource

class NHentai(GalleryDLSource, source_name = "nhentai.net"):
    """Support for nhentai.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["gallery"]
    
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return []

    @override
    def id(self, metadata: dict[str, Any]) -> str:
        """A unique identifier for the post."""
        return str(metadata["gallery_id"])
