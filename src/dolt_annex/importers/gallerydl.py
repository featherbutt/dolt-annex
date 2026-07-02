#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing_extensions import Optional, override

from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.file_io import Path
from dolt_annex.file_keys.base import Sha256E
from .base import Importer

# Remove subcategory and sort keys when importing post
class GalleryDL(Importer):
    """
    Importer for gallery-dl downloads.

    This importer assumes that the files are organized in a specific directory structure
    that is not described here. This exists for historical reasons.
    """

    source: str

    def __init__(self, source: str):
        self.source = source

    @override
    async def key_columns(self, path: Path) -> Optional[TableRow]:
        source = self.source_name(path)
        updated: str | int
        id, updated = path.stem.split('_', 1)
        file_key = await Sha256E.from_file(path)
        table_row = TableRow({
            "source": source,
            "id": id
        })
        if self.table_name(path) == "submissions":
            table_row["part"] = 1
            table_row["metadata_file_key"] = bytes(file_key)
            return table_row
        else:
            table_row["file_key"] = bytes(file_key)
            return table_row

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
