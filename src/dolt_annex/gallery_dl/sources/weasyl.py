#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, Iterable, override

from dolt_annex.datatypes.common import TableRow
from .base import GalleryDLSource

class Weasyl(GalleryDLSource):
    """Support for weasyl.com"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["submission"]
    
    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow(( "weasyl.com", metadata["id"], metadata["date"], 1))

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
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        return [TableRow(( "weasyl.com", metadata["id"], metadata["date"]))]

    @override
    def format_post_metadata(self, metadata: dict[str, Any]):
        super().format_post_metadata(metadata)

        metadata["id"] = metadata.pop("submitid")
