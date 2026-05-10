#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from typing_extensions import Any, override

from .base import GalleryDLSource, UnionPathSelector, mutate_remove_fields

CDN_URL = re.compile("https://(\\w+)\\.ib\\.metapix\\.net/(\\S*)")

class Inkbunny(GalleryDLSource, source_name = "inkbunny.net"):
    """Support for inkbunny.net"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["post", "user", "search"]
    
    @override
    def fields_to_remove(self) -> UnionPathSelector:
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
            "last_file_update_datetime_usertime",
            "create_datetime_usertime",
            ( "pools", [
                "submission_left_file_name",
                "submission_left_submission_id",
                "submission_left_thumb_huge_x",
                "submission_left_thumb_huge_y",
                "submission_left_thumb_large_x",
                "submission_left_thumb_large_y",
                "submission_left_thumb_medium_x",
                "submission_left_thumb_medium_y",
                "submission_left_thumb_huge_noncustom_x",
                "submission_left_thumb_huge_noncustom_y",
                "submission_left_thumb_large_noncustom_x",
                "submission_left_thumb_large_noncustom_y",
                "submission_left_thumb_medium_noncustom_x",
                "submission_left_thumb_medium_noncustom_y",
                "submission_left_thumbnail_url_huge",
                "submission_left_thumbnail_url_huge_noncustom",
                "submission_left_thumbnail_url_large",
                "submission_left_thumbnail_url_large_noncustom",
                "submission_left_thumbnail_url_medium",
                "submission_left_thumbnail_url_medium_noncustom",
                "submission_right_file_name",
                "submission_right_submission_id",
                "submission_right_thumb_huge_x",
                "submission_right_thumb_huge_y",
                "submission_right_thumb_large_x",
                "submission_right_thumb_large_y",
                "submission_right_thumb_medium_x",
                "submission_right_thumb_medium_y",
                "submission_right_thumb_huge_noncustom_x",
                "submission_right_thumb_huge_noncustom_y",
                "submission_right_thumb_large_noncustom_x",
                "submission_right_thumb_large_noncustom_y",
                "submission_right_thumb_medium_noncustom_x",
                "submission_right_thumb_medium_noncustom_y",
                "submission_right_thumbnail_url_huge",
                "submission_right_thumbnail_url_huge_noncustom",
                "submission_right_thumbnail_url_large",
                "submission_right_thumbnail_url_large_noncustom",
                "submission_right_thumbnail_url_medium",
                "submission_right_thumbnail_url_medium_noncustom",
            ])
        ]

    @override
    def id(self, metadata: dict[str, Any]) -> str:
        """A unique identifier for the post."""
        return metadata["submission_id"]
    
    @override
    def format_post_metadata(self, metadata: dict[str, Any]):
        """
        A bug in a previous version of gallery-dl would accidentally include an erroneous "Keywords" tag.
        """
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)
        for key, value in metadata.items():
            if not isinstance(value, str):
                continue
            match = CDN_URL.fullmatch(value)
            if match:
                metadata[key] = f"https://tx.ib.metapix.net/{match.group(2)}"

    format_file_metadata = format_post_metadata