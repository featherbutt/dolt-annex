class DoltAnnexApplicationError(Exception):
    """
    Base class for all Dolt Annex related errors.

    These errors represent application-level issues encountered while using Dolt Annex.
    They should be caught and handled by the application code so that a meaningful message
    can be presented to the user. These errors should not print a stack trace.
    """
    pass

class InvalidFileKeyError(DoltAnnexApplicationError):
    pass
class StorageError(DoltAnnexApplicationError):
    pass

class ConfigurationError(DoltAnnexApplicationError):
    pass

class ContentAddressableStorageError(StorageError):
    """Raised when a provided FileKey does not match the computed FileKey of the data being uploaded."""
    pass

class GalleryDlError(DoltAnnexApplicationError):
    pass
