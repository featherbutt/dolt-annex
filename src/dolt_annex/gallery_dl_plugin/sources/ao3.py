#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, override

from .base import GalleryDLSource

class AO3(GalleryDLSource, source_name = "archiveofourown.org"):
    """Support for archiveofourown.org"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["work", "tag", "search"]

    @override
    def updated_date(self, metadata: dict[str, Any]) -> Any:
        return metadata.get("date_completed") or metadata.get("date_updated") or metadata.get("date") or 0

    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "bookmarks",
            "comments",
            "likes",
            "views"
        ]
