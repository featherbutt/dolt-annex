#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, Iterable, override

from dolt_annex.datatypes.common import TableRow
from .base import GalleryDLSource

class AO3(GalleryDLSource):
    """Support for archiveofourown.org"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["work", "tag", "search"]

    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        date = metadata.get("date_completed") or metadata.get("date_updated") or metadata.get("date") or 0
        return TableRow(( "archiveofourown.org", metadata["id"], date, 1))

    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "bookmarks",
            "comments",
            "likes",
            "views"
        ]
    
    @override
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        date = metadata.get("date_completed") or metadata.get("date_updated") or metadata.get("date") or 0
        yield TableRow(( "archiveofourown.org", metadata["id"], date))
