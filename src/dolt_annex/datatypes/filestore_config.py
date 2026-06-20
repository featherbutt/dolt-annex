from dolt_annex.datatypes.pydantic import StrictBaseModel

class FilestoreConfig(StrictBaseModel):
    verify_existing_files_on_write: bool = False