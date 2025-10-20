#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from typing_extensions import Optional, override

from dolt_annex.datatypes.common import TableRow
from .base import ImporterBase

# Remove subcategory and sort keys when importing post
class GalleryDL(ImporterBase):
    """
    Importer for gallery-dl downloads.

    This importer assumes that the files are organized in a specific directory structure
    that is not described here. This exists for historical reasons.
    """

    source: str

    def __init__(self, source: str):
        self.source = source

    @override
    def key_columns(self, path: Path) -> Optional[TableRow]:
        source = self.source_name(path)
        updated: str | int
        id, updated = path.stem.split('_', 1)
        if updated == "None":
            updated = 0
        if self.table_name(path) == "submissions":
            part = 1
            return TableRow((source, int(id), updated, part))
        return TableRow((source, int(id), updated))

    @override
    def table_name(self, path: Path) -> str:
        match path.parts[-4]:
            case "images":
                return "submissions"
            case "image_metadata":
                return "metadata"
            case "posts":
                return "metadata"
            case _:
                raise ValueError(f"Unknown table for path: {path}")
            
    def source_name(self, path: Path) -> str:
        if path.parts[-4] == "posts":
            return self.source + "/posts"
        return self.source
