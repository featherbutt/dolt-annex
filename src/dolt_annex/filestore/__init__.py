#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The logic for interacting with the filestore.

A filestore is an interface that maps FileKeys to files.

Other filestores could be added in the future. Possible candidates include:
- Cloud storage filestores (e.g. S3)
- Remote filestores (SFTP, rsync, etc.)
- Key-value stores (e.g. Redis)
- IPFS
"""

from .base import FileStore, YesNoMaybe

__all__ = ['FileStore', 'YesNoMaybe']
