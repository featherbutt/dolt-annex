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
    subcategory: Optional[str] = None

_gallery_dl_test_context = contextvars.ContextVar[GalleryDLTestContext]("gallery_dl_test_context")

@contextlib.contextmanager
def with_gallery_dl_test_context(context: GalleryDLTestContext):
    token = _gallery_dl_test_context.set(context)
    try:
        yield
    finally:
        _gallery_dl_test_context.reset(token)

@dataclass
class SourceUrl:
    subcategory: str
    url: str
    
@dataclass
class SourceTest:
    test_id: int
    post_url: SourceUrl
    urls: list[SourceUrl]

tests: dict[str, list[SourceTest]] = {
    "furaffinity": [
        SourceTest(
            test_id=42609833,
            post_url=SourceUrl("post", "https://www.furaffinity.net/view/42609833/"),
            urls=[
                SourceUrl("search", "https://www.furaffinity.net/search/?q=%40keywords+bindingsin"),
                SourceUrl("favorite", "https://www.furaffinity.net/favorites/tobiasfeatherbutt/"),
            ],
        ),
        SourceTest(
            test_id=31302258,
            post_url=SourceUrl("post", "https://www.furaffinity.net/view/31302258/"),
            urls=[
                SourceUrl("gallery", "https://www.furaffinity.net/gallery/entvanen/"),
                SourceUrl("gallery", "https://www.furaffinity.net/user/entvanen/"),
            ],
        ),
    ],
    "pixiv": [
        SourceTest(
            test_id=204505,
            post_url=SourceUrl("work", "https://www.pixiv.net/en/artworks/204505"),
            urls=[
                SourceUrl("search", "https://www.pixiv.net/en/tags/%E7%9F%B3%E6%AE%B5/artworks?order=date")
            ],
        ),
        SourceTest(
            test_id=24254334,
            post_url=SourceUrl("work", "https://www.pixiv.net/en/artworks/24254334"),
            urls=[
                SourceUrl("artworks", "https://www.pixiv.net/en/users/39130")
            ],
        ),
    ],
}

# Sources that require authentication are skipped in CI
skipped_sources = [
    "pixiv",
]

def get(url: str, test_id: int, subcategory: Optional[str] = None) -> dict:
    config_path = pathlib.Path(__file__).parent.parent / "gallery_dl_test_config.json"

    # -N file:id allows us to skip downloading files since we only care about the metadata
    sys.argv = [ "gallery-dl", "-N", "file:id", "--config", str(config_path), url ]

    gallery_dl_test_context = GalleryDLTestContext(target_post_id=test_id)
    with (
        with_gallery_dl_test_context(gallery_dl_test_context),
    ):
        gallery_dl.main()

    if gallery_dl_test_context.post_metadata is not None:
        if subcategory is not None:
            assert gallery_dl_test_context.subcategory == subcategory, f"Expected to parse url as {subcategory} subcategory, but found in {gallery_dl_test_context.subcategory} subcategory"
        return gallery_dl_test_context.post_metadata

    pytest.fail(f"Test post with id {test_id} not found in downloaded posts")

@pytest.mark.parametrize("site", tests.keys())
def test_source(temp_dir, site):

    if site in skipped_sources:
        pytest.skip(f"Source {site} is skipped since it requires authentication")

    site_tests = tests[site]

    for test in site_tests:
        post = get(test.post_url.url, test.test_id, test.post_url.subcategory)

        for url in test.urls:
            subcategory_post = get(url.url, test.test_id, url.subcategory)
            assert subcategory_post == post, f"Expected identical metadata for {url.subcategory} download, but got {subcategory_post} and {post}"