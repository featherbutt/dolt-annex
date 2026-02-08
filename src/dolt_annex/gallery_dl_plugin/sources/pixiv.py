#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import override

from .base import GalleryDLSource

class Pixiv(GalleryDLSource, source_name = "pixiv.net"):
    """Support for pixiv.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["artworks", "user", "tags", "work", "search"]
    
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
            "search",
        ]
