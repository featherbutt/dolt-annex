#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, Iterable, override

from dolt_annex.datatypes.common import TableRow
from .base import GalleryDLSource

class Furaffinity(GalleryDLSource):
    """Support for furaffinity.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["post"]
    
    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow(( "furaffinity.net", metadata["id"], metadata["date"]))

    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "comments",
            "favorites",
            "views"
        ]
    
    @override
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        return TableRow(( "furaffinity.net", metadata["id"], metadata["date"], 1))
