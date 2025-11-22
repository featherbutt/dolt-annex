#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Dict

from .base import GalleryDLSource
from .itaku import Itaku
from .furaffinity import Furaffinity
from .ao3 import AO3
from .pixiv import Pixiv

category_to_source : Dict[str, GalleryDLSource]= {
    "itaku": Itaku(),
    "furaffinity": Furaffinity(),
    "ao3": AO3(),
    "pixiv": Pixiv(),
}

def get_source(category: str, subcategory: str) -> GalleryDLSource:
    if not (source := category_to_source.get(category)):
        raise ValueError(f"Category {category} is not currently supported. Supported categories: {list(category_to_source.keys())}")
    if subcategory not in source.supported_subcategories():
        raise ValueError(f"Subcategory {subcategory} is not supported for category {category}. Supported subcategories: {source.supported_subcategories()}")
    return source