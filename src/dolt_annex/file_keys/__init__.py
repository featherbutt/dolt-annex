#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Logic for computing keys for files.
"""

import logging
from typing_extensions import Annotated

from pydantic import ModelWrapValidatorHandler, PlainSerializer, WrapValidator

from .base import FileKey, Sha256E, MD5e, SHA1e, Sha1HSe, Sha256HSe, MD5HSe

logger = logging.getLogger(__name__)

def file_key_type_validator(name, _: ModelWrapValidatorHandler[type[FileKey]]) -> type[FileKey]:
    """Get the FileKey subclass for the given key format name."""
    if isinstance(name, type) and issubclass(name, FileKey):
        return name
    return get_file_key_type(str(name))

def get_file_key_type(name: str) -> type[FileKey]:
    """Get the FileKey subclass for the given key format name."""
    if name not in FileKey.prefixes:
        raise ValueError(f"Unknown file key type: {name}")
    return FileKey.prefixes[name]

FileKeyType = Annotated[type[FileKey], WrapValidator(file_key_type_validator), PlainSerializer(lambda t: t.prefix)]

__all__ = ['FileKey', 'FileKeyType', 'Sha256E', 'MD5e', 'SHA1e', 'Sha1HSe', 'Sha256HSe', 'MD5HSe']
