from typing import List

from dolt_annex.datatypes.collection import Collection
from dolt_annex.datatypes.pydantic import StrictBaseModel

class GalleryDLConfig(StrictBaseModel):
    capture_output: bool = False
    skip_download: bool = False
    collections: List[Collection] = []
