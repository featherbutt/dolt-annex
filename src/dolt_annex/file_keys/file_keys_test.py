#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from typing_extensions import Dict

from .base import FileKey
from .base import Sha1HSe, Sha256HSe, MD5HSe
from .base import SHA1e, Sha256E, MD5e

# All these keys correspond to the same file, the simple string "hello world"

keys: Dict[type[FileKey], bytes] = {
    Sha256E: b'SHA256E-s11--b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9.TXT',
    SHA1e: b'SHA1e-s11--2aae6c35c94fcfb415dbe95f408b9ce91ee846ed.txt',
    MD5e: b'MD5e-s11--5eb63bbbe01eeed093cb22bb8f5acdc3.txt',
    Sha1HSe: b'SHA1_HSe-2aae6c35c94fcfb415dbe95f408b9ce91ee846ed--s11.txt',
    Sha256HSe: b'SHA256_HSe-b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9--s11.txt',
    MD5HSe: b'MD5_HSe-5eb63bbbe01eeed093cb22bb8f5acdc3--s11.txt',
}

def key(key_type: type[FileKey]) -> FileKey:
    return FileKey.must_parse(keys[key_type])

def test_file_keys():
    for KeyType, keyName in keys.items():
        assert KeyType.from_bytes(b"hello world", extension="TXT") == FileKey.must_parse(keyName)

def conversion_test_cases():
    return [
        (Sha256E, Sha256HSe, True),
        (Sha256HSe, Sha256E, True),
        (Sha1HSe, SHA1e, True),
        (SHA1e, Sha1HSe, True),
        (MD5HSe, MD5e, True),
        (MD5e, MD5HSe, True),
        (Sha256E, SHA1e, False),
        (SHA1e, MD5e, False),
        (MD5HSe, Sha256E, False),
    ]

@pytest.mark.parametrize("from_type, to_type, is_convertable", conversion_test_cases())
def test_convertable_from(from_type: type[FileKey], to_type: type[FileKey], is_convertable: bool):
    assert to_type.convertable_from(from_type) == is_convertable
    if is_convertable:
        assert to_type.convert_from(key(from_type)).same_bytes(key(to_type))
        if to_type is not Sha256E:
            # Converting to Sha256E can lose case sensitivity in the extension. This is acceptable.
            assert to_type.convert_from(key(from_type)) == key(to_type)
            assert to_type.convert_from(key(from_type).remove_extension()) == key(to_type).remove_extension()
        
    