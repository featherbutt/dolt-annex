#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from typing import ClassVar, NewType, Optional
from typing_extensions import Any, Iterable

from dolt_annex.datatypes.common import TableRow
from dolt_annex.file_keys.base import FileKey, FileKeyPrefix

type SequencePathSelector = 'tuple[str | UnionPathSelector, ...]'
type UnionPathSelector = 'list[str | SequencePathSelector]'
type PathSelector = str | SequencePathSelector | UnionPathSelector

def is_private_field(field: str) -> bool:
    """Whether a field is considered private and should be excluded from imported metadata."""
    return field.startswith("_")

# PostMetadata describes the metadata for a post, which may contain multiple files and has a page count.
# FileMetadata describes the metadata for a single file, which may be part of a post, and has a page index.
PostMetadata = NewType("PostMetadata", dict[str, Any])
FileMetadata = NewType("FileMetadata", dict[str, Any])

class GalleryDLSource:
    """A supported gallery-dl source. Methods describe how to parse and process metadata from that source."""

    source_name: ClassVar[str]

    def __init_subclass__(cls, source_name: str, **kwargs):
        super().__init_subclass__(**kwargs)

        cls.source_name = source_name

    @abstractmethod
    def supported_subcategories(self) -> list[str]:
        """
        The subcategories supported by this source.

        Before adding a subcategory here, please ensure that:
        - Submissions have identical metadata when downloaded from this subcategory's endpoint vs. other endpoints.
        - Any fields that change frequently (e.g. view counts) are removed in `fields_to_remove`.
        """

    def exclude_field(self, field: str) -> bool:
        """Whether to exclude a given field from the imported metadata."""
        return is_private_field(field) or field == "subcategory" or field in self.fields_to_remove()

    @abstractmethod
    def fields_to_remove(self) -> PathSelector:
        """
        A list of fields that will be removed from the imported metadata.
        
        In general, imported metadata should:
        - Be identical regardless of the endpoint used to fetch it (e.g. search results vs. individual submission)
        - Only change when the submission itself changes (excluding things like view counts)
        """

    def format_file_metadata(self, metadata: FileMetadata):
        """Format the metadata in a source-specific way. Can be overridden by implementations."""
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)

    def format_post_metadata(self, metadata: PostMetadata):
        """Format the metadata in a source-specific way. Can be overridden by implementations."""
        mutate_remove_fields(metadata, self.fields_to_remove())
        metadata["_id"] = self.id(metadata)

    def file_metadata(self, metadata: FileMetadata) -> Iterable[TableRow]:
        """The table row for 'file' metadata, if any."""
        return []

    def id(self, metadata: PostMetadata | FileMetadata) -> str:
        """A unique identifier for the post."""
        return str(metadata["id"])

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
    
    def keys_from_metadata(self, metadata: FileMetadata) -> Iterable[FileKey | FileKeyPrefix]:
        return []
    
    def file_url_from_metadata(self, metadata: FileMetadata) -> Optional[Iterable[str]]:
        return None
    
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

def mutate_remove_fields(d: dict | list, field_to_remove: PathSelector):
    """
    Modifies a JSON object to remove all fields matching the provided selector.
    """
    if isinstance(d, list):
        # If the input object is a list, simply apply the selector to each of its elements.
        for item in d:
            mutate_remove_fields(item, field_to_remove)
        return
    
    # Here, |d| must be a dict.
    if isinstance(field_to_remove, str):
        if field_to_remove in d:
            del d[field_to_remove]
        return
    
    # |field_to_remove| is either a UnionPathSelector or a SequencePathSelector
    # In either case, if it only has a single child selector, recurse on that child.
    if len(field_to_remove) == 1:
        return mutate_remove_fields(d, next(iter(field_to_remove)))
        
    if isinstance(field_to_remove, list):
        # The selector is a UnionPathSelector: recursively apply each selector.
        for field in field_to_remove:
            mutate_remove_fields(d, field)
        return

    # Else the selector is a SequencePathSelector
    # Don't use destructuring syntax here because it would make rest a list.
    field, rest = field_to_remove[0], field_to_remove[1:]
        
    if isinstance(field, str):
        if field in d:
            mutate_remove_fields(d[field], rest)
        return

    # field is a UnionPathSelector
    for possible_field in field:
        if isinstance(possible_field, str):
            if possible_field in d:
                mutate_remove_fields(d[possible_field], rest)
            return
        # possible_field is a SequencePathSelector
        child_rest = (*possible_field[1:], *rest)
        possible_field = possible_field[0]
        mutate_remove_fields(d[possible_field], child_rest)
