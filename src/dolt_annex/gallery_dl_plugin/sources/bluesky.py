#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from typing import ClassVar, NewType, Optional, override
from typing_extensions import Any, Iterable

from dolt_annex.datatypes.common import TableRow
from dolt_annex.file_keys.base import FileKey, FileKeyPrefix

from .base import GalleryDLSource, PathSelector, mutate_remove_fields

# PostMetadata describes the metadata for a post, which may contain multiple files and has a page count.
# FileMetadata describes the metadata for a single file, which may be part of a post, and has a page index.
PostMetadata = NewType("PostMetadata", dict[str, Any])
FileMetadata = NewType("FileMetadata", dict[str, Any])

class Bluesky(GalleryDLSource, source_name = "bluesky"):
   
    @override
    def supported_subcategories(self) -> list[str]:
        return ["post"]

    @override
    def fields_to_remove(self) -> PathSelector:
        return [
            "bookmarkCount",
            "replyCount",
            "repostCount",
            "likeCount",
            "quoteCount",
            ("author", "associated")
        ]

    def file_metadata(self, metadata: FileMetadata) -> Iterable[TableRow]:
        """The table row for 'file' metadata, if any."""
        return []

    def id(self, metadata: PostMetadata | FileMetadata) -> str:
        """A unique identifier for the post."""
        return str(metadata["post_id"])

    def page_number(self, metadata: FileMetadata) -> int:
        """
        The page number of the image within the post, if applicable.
        
        Used to distinguish between multiple files from the same post.
        """
        return metadata.get("num", 1)

    @abstractmethod
    def num_pages(self, metadata: PostMetadata) -> int:
        """
        The page number of the image within the post, if applicable.
        
        Used to distinguish between multiple files from the same post.
        """

    @override
    def keys_from_metadata(self, metadata: FileMetadata) -> Iterable[FileKey | FileKeyPrefix]:
        return []

    @override
    def file_url_from_metadata(self, metadata: FileMetadata) -> Optional[Iterable[str]]:
        raise NotImplementedError()
    
    def assume_same_file(self, left: dict[str, Any], right: dict[str, Any], page_number: int) -> bool:
        """
        Whether or not we can safely assume that two metadata dicts have the same submission data.

        Typically this requires that the metadata contains one of:
        - A "last updated" timestamp that only changes when the submission data changes.
        - A url that only changes when the submission data changes.

        We only call this if the dicts have already been compared unequal.

        Note that if the metadata contains a hash, |keys_from_metadata| works better.
        """
        return False
