#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import abstractmethod
from typing_extensions import Any, Iterable

from dolt_annex.datatypes.common import TableRow

def is_private_field(field: str) -> bool:
    """Whether a field is considered private and should be excluded from imported metadata."""
    return field.startswith("_")

class GalleryDLSource:
    """A supported gallery-dl source. Methods describe how to parse and process metadata from that source."""

    @abstractmethod
    def table_key(self, metadata: dict[str, Any]) -> TableRow:
        """The table key used for submissions from this source."""

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
    def fields_to_remove(self) -> list[str | list[str]]:
        """
        A list of fields that will be removed from the imported metadata.
        
        In general, imported metadata should:
        - Be identical regardless of the endpoint used to fetch it (e.g. search results vs. individual submission)
        - Only change when the submission itself changes (excluding things like view counts)
        """

    def format_file_metadata(self, metadata: dict[str, Any]):
        """Format the metadata in a source-specific way. Can be overridden by implementations."""
        mutate_remove_fields(metadata, self.fields_to_remove())

    def format_post_metadata(self, metadata: dict[str, Any]):
        """Format the metadata in a source-specific way. Can be overridden by implementations."""
        mutate_remove_fields(metadata, self.fields_to_remove())

    @abstractmethod
    def post_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        """The table row for 'post' metadata."""

    def file_metadata(self, metadata: dict[str, Any]) -> Iterable[TableRow]:
        """The table row for 'file' metadata, if any."""
        return []

def mutate_remove_field(d: dict | list, field_to_remove: str | list[str]):
    if isinstance(d, list):
        for item in d:
            mutate_remove_field(item, field_to_remove)
        return
    
    if isinstance(field_to_remove, str):
        if field_to_remove in d:
            del d[field_to_remove]
        return
    
    if len(field_to_remove) == 1:
        if field_to_remove[0] in d:
            del d[field_to_remove[0]]
        return

    field, *rest = field_to_remove

    if field in d:
        mutate_remove_field(d[field], rest)

def mutate_remove_fields(d: dict, fields_to_remove: list[str | list[str]]):
    """Remove fields from a dictionary in place."""
    for field in fields_to_remove:
        mutate_remove_field(d, field)