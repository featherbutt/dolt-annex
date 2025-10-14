#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Any, Iterable, override

from dolt_annex.datatypes.common import TableRow
from .base import GalleryDLSource, mutate_remove_fields

class Itaku(GalleryDLSource):
    """Support for itaku.ee"""

    @override
    def supported_subcategories(self) -> list[str]:
        return ["post", "posts", "image", "images"]
    
    @override
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        return TableRow((
            "itaku.ee/images",
            metadata["id"],
            metadata.get("date_edited") or metadata.get("date_added"),
            1,
        ))

    @override
    def fields_to_remove(self) -> list[str | list[str]]:
        return [
            "liked_by_you",
            "num_likes",
            "too_mature",
            "blacklisted",
            "bookmarked_by_you",
            "num_reshares",
            "num_comments",
            "is_thumbnail_for_video",
            "show_content_warning",
            "reshared_by_you",
            # Some endpoints only return regular and xl images. Removing large makes the output consistent.
            "image_lg",
            "num_too_mature_imgs",
            ["folders", "num_posts"],
        ]

    @override
    def format_post_metadata(self, metadata: dict[str, Any]):
        super().format_post_metadata(metadata)

        if (gallery_images := metadata.get("gallery_images")):
            for image in gallery_images:
                mutate_remove_fields(image, self.fields_to_remove())

    @override
    def format_file_metadata(self, metadata: dict[str, Any]):
        match metadata.get("subcategory"):
            case "post":
                # Move image metadata to the top level and remove unneeded fields.
                file: dict = metadata.get("file", {})
                mutate_remove_fields(file, self.fields_to_remove())
                for field in list(metadata.keys()):
                    if not field.startswith("_") and field not in ("category", "subcategory", "filename", "extension"):
                        del metadata[field]
                metadata.update(file)
            case _:
                super().format_post_metadata(metadata)

    @override
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        if metadata.get("subcategory") == "post":
            yield TableRow((
                "itaku.ee/posts",
                metadata["id"],
                metadata.get("date_edited") or metadata.get("date_added"),
            ))

    @override
    def file_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        yield TableRow((
            "itaku.ee/images",
            metadata["id"],
            metadata.get("date_edited") or metadata.get("date_added"),
        ))
