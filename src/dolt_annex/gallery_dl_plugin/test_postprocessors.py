#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A gallery-dl postprocessor for testing that sources are correctly formatting metadata for dolt-annex.
"""

import gallery_dl
from dolt_annex.gallery_dl_plugin.sources.source_test import _gallery_dl_test_context

from .sources import get_source

def gallery_dl_post_test(metadata: dict):
    category = metadata["category"]
    subcategory = metadata["subcategory"]
    source = get_source(category, subcategory)

    source.format_post_metadata(metadata)

    context = _gallery_dl_test_context.get()

    public_metadata = { k: v for k, v in metadata.items() if not source.exclude_field(k) }

    if public_metadata["id"] == context.target_post_id:
        context.post_metadata = public_metadata
        raise gallery_dl.exception.TerminateExtraction()