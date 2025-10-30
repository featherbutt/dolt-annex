#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The logic for interacting with the filestore.

A filestore is an interface that maps AnnexKeys to files.

Currently the only supported filestore is a local filesystem filestore,
which stores files in a directory structure based on the md5 hash of the key.

If the md5 hash of a key <KEY> is abcdef123456..., then the file is stored at abc/def/<KEY>.

Other filestores could be added in the future. Possible candidates include:
- Cloud storage filestores (e.g. S3)
- Remote filestores (SFTP, rsync, etc.)
- Key-value stores (e.g. Redis)
- IPFS
"""

from .base import FileStore, YesNoMaybe
from .annexfs import AnnexFS

__all__ = ['FileStore', 'AnnexFS', 'YesNoMaybe']
