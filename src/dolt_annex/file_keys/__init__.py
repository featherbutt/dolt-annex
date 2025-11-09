#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Logic for computing keys for files.
Currently the only supported key scheme is git-annex's SHA256E keys.
But other schemes could be added in the future.
"""

import importlib
from pydantic import ModelWrapValidatorHandler, PlainSerializer, ValidateAs, WrapValidator
from typing_extensions import Annotated, Optional
from .base import FileKey
from .sha256e import Sha256e

def file_key_type_validator(name, _: ModelWrapValidatorHandler[type[FileKey]]) -> type[FileKey]:
    """Get the FileKey subclass for the given key format name."""
    if isinstance(name, type) and issubclass(name, FileKey):
        return name
    return get_file_key_type(str(name))

def get_file_key_type(name: str) -> type[FileKey]:
    """Get the FileKey subclass for the given key format name."""
    class_name: Optional[str]
    match name.split('.'):
        case [module_name]:
            class_name = None
        case [module_name, class_name]:
            pass
        case _:
            raise ImportError(f"Unsupported key format: {name}")
    file_key_module = importlib.import_module(f".{module_name.lower()}", package=__name__)
    if class_name:
        return getattr(file_key_module, class_name)
    return getattr(file_key_module, module_name)

type FileKeyType = Annotated[type[FileKey], WrapValidator(file_key_type_validator), PlainSerializer(lambda t: t.__name__)]

__all__ = ['FileKey', 'FileKeyType', 'Sha256e']
