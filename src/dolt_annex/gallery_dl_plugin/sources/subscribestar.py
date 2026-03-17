#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any
from typing_extensions import override

from .base import GalleryDLSource

class SubscribeStar(GalleryDLSource, source_name = "subscribestar.com"):
    """Support for subscribestar.com"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["user-adult"]
    
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return []
    
    def id(self, metadata: dict[str, Any]) -> str:
        """A unique identifier for the post."""
        print(metadata)
        return str(metadata["post_id"])

