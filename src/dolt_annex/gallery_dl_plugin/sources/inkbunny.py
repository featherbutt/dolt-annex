#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, Iterable, override

from dolt_annex.datatypes.common import TableRow
from .base import GalleryDLSource

class Inkbunny(GalleryDLSource):
    """Support for inkbunny.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["post", "user", "search"]
    
    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow(( "inkbunny.net", metadata["submission_id"], metadata["date"], metadata["num"]))
    
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "comments_count",
            "favorites_count",
            "guest_block",
            "hidden",
            "views",
            "watching",
            "user_icon_file_name",
            "user_icon_url_large",
            "user_icon_url_medium",
            "user_icon_url_small",
            "search",
        ]
    
    @override
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        return [TableRow(( "inkbunny.net", metadata["submission_id"], metadata["date"]))]

    @override
    def id(self, metadata: dict[str, Any]) -> str:
        """A unique identifier for the post."""
        return metadata["submission_id"]