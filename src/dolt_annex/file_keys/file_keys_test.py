#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing_extensions import Dict

from .base import FileKey
from .hash_size_extension import Sha1HSe, Sha256HSe, MD5HSe
from .size_hash_extension import SHA1e, Sha256E, MD5e

# All these keys correspond to the same file, the simple string "hello world"

keys: Dict[type[FileKey], bytes] = {
    Sha256E: b'SHA256E-s11--b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9.TXT',
    SHA1e: b'SHA1e-s11--2aae6c35c94fcfb415dbe95f408b9ce91ee846ed.txt',
    MD5e: b'MD5e-s11--5eb63bbbe01eeed093cb22bb8f5acdc3.txt',
    Sha1HSe: b'SHA1_HSe-2aae6c35c94fcfb415dbe95f408b9ce91ee846ed--s11.txt',
    Sha256HSe: b'SHA256_HSe-b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9--s11.txt',
    MD5HSe: b'MD5_HSe-5eb63bbbe01eeed093cb22bb8f5acdc3--s11.txt',
}

def test_file_keys():
    for KeyType, keyName in keys.items():
        assert KeyType.from_bytes(b"hello world", extension="TXT") == FileKey.must_parse(keyName)
