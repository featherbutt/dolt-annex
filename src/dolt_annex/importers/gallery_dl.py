#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from typing_extensions import Optional, override

from dolt_annex.datatypes.common import TableRow
from .base import ImporterBase

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
        id, updated = path.stem.split('_', 1)
        if self.table_name(path) == "submissions":
            part = 1
            return TableRow((self.source, int(id), updated, part))
        return TableRow((self.source, int(id), updated))

    @override
    def table_name(self, path: Path) -> str:
        match path.parts[-4]:
            case "images":
                return "submissions"
            case "image_metadata":
                return "metadata"
            case "posts":
                return "posts"
            case _:
                raise ValueError(f"Unknown table for path: {path}")