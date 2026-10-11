#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
from typing import Any, List, Self

from typing_extensions import Dict

from dolt_annex.datatypes.errors import GalleryDlError

from .base import GalleryDLSource
from .itaku import Itaku
from .furaffinity import Furaffinity
from .inkbunny import Inkbunny
from .ao3 import AO3
from .pixiv import Pixiv
from .nhentai import NHentai
from .e621 import E621
from .subscribestar import SubscribeStar
from .gelbooru import Gelbooru

sources: List[GalleryDLSource] = [
    Itaku(),
    Furaffinity(),
    AO3(),
    Pixiv(),
    Inkbunny(),
    NHentai(),
    E621(),
    SubscribeStar(),
    Gelbooru(),
]

category_to_source : Dict[str, GalleryDLSource]= {
    "itaku": Itaku(),
    "furaffinity": Furaffinity(),
    "ao3": AO3(),
    "pixiv": Pixiv(),
    "inkbunny": Inkbunny(),
    "nhentai": NHentai(),
    "e621": E621(),
    "subscribestar": SubscribeStar(),
    "gelbooru": Gelbooru(),
}

class GalleryDlCategoryNotSupportedError(GalleryDlError):
    @classmethod
    def new(cls, category: str) -> Self:
        message = f"""Category {category} is supported by gallery-dl but not currently supported by dolt-annex. If you would like it to be supported, please open an issue.

Supported categories:
{'\n'.join(f' - {key}' for key in sorted(category_to_source.keys()))}
"""
        return cls(message)

class GalleryDlSubcategoryNotSupportedError(GalleryDlError):
    @classmethod
    def new(cls, source: GalleryDLSource, category: str, subcategory: str) -> Self:
        message = f"""Subcategory {subcategory} is for category {category} is supported by gallery-dl but not currently supported by dolt-annex. If you would like it to be supported, please open an issue.

Supported subcategories for {category}:
{'\n'.join(f' - {key}' for key in sorted(source.supported_subcategories()))}
"""
        return cls(message)

def get_source(category: str, subcategory: str) -> GalleryDLSource:
    if not (source := category_to_source.get(category)):
        raise GalleryDlCategoryNotSupportedError.new(category)
    if subcategory not in source.supported_subcategories():
        raise GalleryDlSubcategoryNotSupportedError.new(source, category, subcategory)
    return source

def remove_metadata(metadata: Dict[str, Any]):
    new_metadata = copy.deepcopy(metadata)
    category = new_metadata["category"]
    source = category_to_source[category]
    source.format_file_metadata(new_metadata)
    return new_metadata
