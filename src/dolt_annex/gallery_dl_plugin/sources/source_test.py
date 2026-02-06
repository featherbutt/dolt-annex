#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import contextvars
from dataclasses import dataclass
import sys
import pathlib
from typing_extensions import Optional

import gallery_dl
import pytest

# For each source, provide a sample URL for each supported subcategory.
# These tests ensure that gallery-dl can successfully download from each subcategory,
# and that the provided id is downloaded for each subcategory with identical metadata.

@dataclass
class GalleryDLTestContext:
    target_post_id: int = 0
    post_metadata: Optional[dict] = None

_gallery_dl_test_context = contextvars.ContextVar[GalleryDLTestContext]("gallery_dl_test_context")

@contextlib.contextmanager
def with_gallery_dl_test_context(context: GalleryDLTestContext):
    token = _gallery_dl_test_context.set(context)
    try:
        yield
    finally:
        _gallery_dl_test_context.reset(token)

@dataclass
class SourceTest:
    test_id: int
    post_url: str
    urls: dict[str, str]  # subcategory -> url

tests: dict[str, list[SourceTest]] = {
    "furaffinity": [
        SourceTest(
            test_id=42609833,
            post_url="https://www.furaffinity.net/view/42609833/",
            urls={
                "search": "https://www.furaffinity.net/search/?q=%40keywords+bindingsin",
                "favorite": "https://www.furaffinity.net/favorites/tobiasfeatherbutt/",
            },
        ),
        SourceTest(
            test_id=31302258,
            post_url="https://www.furaffinity.net/view/31302258/",
            urls={
                "gallery": "https://www.furaffinity.net/gallery/entvanen/",
                "user": "https://www.furaffinity.net/user/entvanen/",
            },
        ),
    ]
}

def get(url: str, test_id: int) -> dict:
    config_path = pathlib.Path(__file__).parent.parent / "gallery_dl_test_config.json"

    # -N file:id allows us to skip downloading files since we only care about the metadata
    sys.argv = [ "gallery-dl", "-N", "file:id", "--config", str(config_path), url ]

    gallery_dl_test_context = GalleryDLTestContext(target_post_id=test_id)
    with (
        with_gallery_dl_test_context(gallery_dl_test_context),
    ):
        gallery_dl.main()

    if gallery_dl_test_context.post_metadata is not None:
        return gallery_dl_test_context.post_metadata

    pytest.fail(f"Test post with id {test_id} not found in downloaded posts")

@pytest.mark.parametrize("site", tests.keys())
def test_source(temp_dir, site):

    site_tests = tests[site]

    for test in site_tests:
        post = get(test.post_url, test.test_id)

        for subcategory, url in test.urls.items():
            subcategory_post = get(url, test.test_id)
            assert subcategory_post == post, f"Expected identical metadata for {subcategory} download, but got {subcategory_post} and {post}"
