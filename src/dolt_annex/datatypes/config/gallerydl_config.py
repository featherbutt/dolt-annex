from typing_extensions import Protocol

from dolt_annex.datatypes.pydantic import StrictBaseModel

class GalleryDLConfig(StrictBaseModel):
    capture_output: bool = False
    skip_download: bool = False
