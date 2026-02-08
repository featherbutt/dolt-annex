#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Logic for computing keys for files.
"""

from pydantic import ModelWrapValidatorHandler, PlainSerializer, WrapValidator
from typing_extensions import Annotated
from .base import FileKey
from .size_hash_extension import Sha256E, MD5e, SHA1e

def file_key_type_validator(name, _: ModelWrapValidatorHandler[type[FileKey]]) -> type[FileKey]:
    """Get the FileKey subclass for the given key format name."""
    if isinstance(name, type) and issubclass(name, FileKey):
        return name
    return get_file_key_type(str(name))

def get_file_key_type(name: str) -> type[FileKey]:
    """Get the FileKey subclass for the given key format name."""
    return FileKey.prefixes[name]

FileKeyType = Annotated[type[FileKey], WrapValidator(file_key_type_validator), PlainSerializer(lambda t: t.prefix)]

__all__ = ['FileKey', 'FileKeyType', 'Sha256E', 'MD5e', 'SHA1e']
