#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, Iterable, override

from dolt_annex.datatypes.common import TableRow
from .base import GalleryDLSource

class Pixiv(GalleryDLSource):
    """Support for pixiv.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["artworks", "user", "tags"]
    
    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow(("pixiv.net", metadata["id"], metadata["date"], metadata["num"]))
        
    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "total_view",
            "total_bookmarks",
            "is_bookmarked",
            "is_muted",
            "seasonal_effect_animation_urls",
            "event_banners",
            "total_comments",
            "comment_access_control",
            "profile",
            "profile_publicity",
            "workspace",
            "restriction_attributes",
            ["user", "is_followed"],
            ["user", "is_access_blocking_user"],
        ]
    
    @override
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        yield TableRow(("pixiv.net", metadata["id"], metadata["date"]))

    @override
    def file_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        yield TableRow(("pixiv.net", metadata["id"], metadata["date"], metadata["num"]))
