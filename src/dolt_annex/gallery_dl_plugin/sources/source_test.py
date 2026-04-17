#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import contextvars
from dataclasses import dataclass, field
import importlib
import sys
import pathlib
from typing_extensions import Optional

import gallery_dl
import pytest

from dolt_annex.file_keys.base import FileKey
from dolt_annex.file_keys import Sha256E
from dolt_annex.gallery_dl_plugin.sources.base import GalleryDLSource
from dolt_annex.gallery_dl_plugin.sources.e621 import E621
from dolt_annex.gallery_dl_plugin.sources.furaffinity import Furaffinity
from dolt_annex.gallery_dl_plugin.sources.inkbunny import Inkbunny
from dolt_annex.gallery_dl_plugin.sources.pixiv import Pixiv
from dolt_annex.gallery_dl_plugin.sources.nhentai import NHentai
from dolt_annex.test_util import EnvironmentForTest
from dolt_annex.table import Dataset
from dolt_annex.gallery_dl_plugin import make_default_schema, run_gallery_dl

# For each source, provide a sample URL for each supported subcategory.
# These tests ensure that gallery-dl can successfully download from each subcategory,
# and that the provided id is downloaded for each subcategory with identical metadata.

@dataclass
class GalleryDLTestContext:
    target_post_id: str
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
class MetadataTest:
    """A test that a particular post can be downloaded from multiple URLS with identical metadata."""
    test_id: str
    post_url: SourceUrl
    urls: list[SourceUrl]

@dataclass
class TableRow:
    file_key: FileKey
    part: int
    metadata_file_key: Optional[FileKey] = None

@dataclass
class ImportTest:
    """A test that a particular post can imported into the database."""
    post_url: SourceUrl
    id: int
    rows: list[TableRow]

@dataclass
class SourceTests:
    """All tests for a particular source."""
    metadata_tests: list[MetadataTest] = field(default_factory=list)
    import_tests: list[ImportTest] = field(default_factory=list)

tests: dict[type[GalleryDLSource], SourceTests] = {
    Furaffinity: SourceTests(
        metadata_tests=[
            MetadataTest(
                test_id="42609833",
                post_url=SourceUrl("post", "https://www.furaffinity.net/view/42609833/"),
                urls=[
                    SourceUrl("search", "https://www.furaffinity.net/search/?q=%40keywords+bindingsin"),
                    SourceUrl("favorite", "https://www.furaffinity.net/favorites/tobiasfeatherbutt/"),
                ],
            ),
            MetadataTest(
                test_id="31302258",
                post_url=SourceUrl("post", "https://www.furaffinity.net/view/31302258/"),
                urls=[
                    SourceUrl("gallery", "https://www.furaffinity.net/gallery/entvanen/"),
                    SourceUrl("gallery", "https://www.furaffinity.net/user/entvanen/"),
                ],
            ),
        ],
    ),
    Pixiv: SourceTests(
        metadata_tests=[
            MetadataTest(
                test_id="204505",
                post_url=SourceUrl("work", "https://www.pixiv.net/en/artworks/204505"),
                urls=[
                    SourceUrl("search", "https://www.pixiv.net/en/tags/%E7%9F%B3%E6%AE%B5/artworks?order=date")
                ],
            ),
            MetadataTest(
                test_id="24254334",
                post_url=SourceUrl("work", "https://www.pixiv.net/en/artworks/24254334"),
                urls=[
                    SourceUrl("artworks", "https://www.pixiv.net/en/users/39130")
                ],
            ),
        ],
        import_tests=[
            ImportTest(
                post_url=SourceUrl("work", "https://www.pixiv.net/en/artworks/204505"),
                id=204505,
                rows=[TableRow(
                    file_key=Sha256E(key=b"SHA256E-s413096--6e7e59d329d0aa2e3c33e48c7718e1557f46e943591a05a9521c7b52bb090020.jpg"),
                    part=1
                )],
            )
        ],
    ),
    Inkbunny: SourceTests(
        metadata_tests=[
            MetadataTest(
                test_id="2859344",
                post_url=SourceUrl("post", "https://inkbunny.net/s/2859344"),
                urls=[
                    SourceUrl("user", "https://inkbunny.net/bindingsin/"),
                    SourceUrl("search", "https://inkbunny.net/submissionsviewall.php?mode=search&page=1&orderby=&text=0c3af53e7b6bee504f853914d70ec0cc&md5=yes")
                ],
            ),
        ],
        import_tests=[
            ImportTest(
                post_url=SourceUrl("post", "https://inkbunny.net/s/3822970"),
                id=3822970,
                rows=[TableRow(
                    file_key=Sha256E(key=b"SHA256E-s523155--43676b493605afd86558acc69b6b38e9a8d351c01b666bbf917541c53dc0deaf.jpg"),
                    part=1,
                ),
                TableRow(
                    file_key=Sha256E(key=b"SHA256E-s1166228--397140a1e36cd2392e40f4896fc99e7e31c923057168ae78661c7257ea42c869.gif"),
                    part=2,
                )],
            )
        ],
    ),
    NHentai: SourceTests(
        metadata_tests=[
            MetadataTest(
                test_id="454571",
                post_url=SourceUrl("gallery", "https://nhentai.net/g/454571/"),
                urls=[
                    SourceUrl("gallery", "https://nhentai.net/tag/aira-shiratori/"),
                    SourceUrl("gallery", "https://nhentai.net/search/?q=invincible+onigiri")
                ],
            ),
        ],
        import_tests=[
            ImportTest(
                post_url=SourceUrl("gallery", "https://nhentai.net/g/625661/"),
                id=625661,
                rows=[TableRow(
                    file_key=Sha256E(key=b"SHA256E-s71612--67f7f28a088200063e4bd41773595ae02d692c4ea9934ea46aa1cb79972b524a.webp"),
                    part=1,
                )],
            )
        ],
    ),
    E621: SourceTests(
        metadata_tests=[
            MetadataTest(
                test_id="14",
                post_url=SourceUrl("post", "https://e621.net/posts/14"),
                urls=[
                    SourceUrl("tag", "https://e621.net/posts?tags=incendax+order%3Aid"),
                ],
            ),
        ],
        import_tests=[
            ImportTest(
                post_url=SourceUrl("post", "https://e621.net/posts/14"),
                id=14,
                rows=[TableRow(
                    file_key=Sha256E(key=b"SHA256E-s96998--8dc0383e01b3ff0b4af51ba57159b81557090664dbe350398ae2db2b72094c08.jpg"),
                    part=1,
                    metadata_file_key=Sha256E(key=b"SHA256E-s5802--3e73fa65dce1312c5f657108a78301ab4bedb14715e7173a54bf3b1050b611b7.json"),
                )],
            )
        ],
    ),
}



# Sources that require authentication are skipped in CI
skipped_sources = [
    Pixiv,
    Furaffinity,
]

def get_metadata(url: str, test_id: str, subcategory: Optional[str] = None) -> dict:
    config_path = pathlib.Path(__file__).parent.parent / "gallery_dl_test_config.json"

    # -N file:id allows us to skip downloading files since we only care about the metadata
    sys.argv = [ "gallery-dl", "-v", "-N", "file:id", "--config", str(config_path), url ]

    gallery_dl_test_context = GalleryDLTestContext(target_post_id=test_id)
    with (
        with_gallery_dl_test_context(gallery_dl_test_context),
    ):
        # Clear gallery_dl's internal state to avoid interference between tests.
        gallery_dl.config.clear()
        gallery_dl.main()

    if gallery_dl_test_context.post_metadata is not None:
        if subcategory is not None:
            assert gallery_dl_test_context.subcategory == subcategory, f"Expected to parse url as {subcategory} subcategory, but found in {gallery_dl_test_context.subcategory} subcategory"
        return gallery_dl_test_context.post_metadata

    pytest.fail(f"Test post with id {test_id} not found in downloaded posts")

@pytest.mark.parametrize("site", tests.keys())
def test_source_metadata(temp_dir, site):
    """Test that we can download metadata for a post from multiple subcategories with identical results."""

    importlib.reload(gallery_dl)
    if site in skipped_sources:
        pytest.skip(f"Source {site} is skipped since it requires authentication")

    for test in tests[site].metadata_tests:
        post = get_metadata(test.post_url.url, test.test_id, test.post_url.subcategory)

        for url in test.urls:
            subcategory_post = get_metadata(url.url, test.test_id, url.subcategory)
            assert subcategory_post == post, f"Expected identical metadata for {url.subcategory} download, but got {subcategory_post} and {post}"

@pytest.mark.asyncio
@pytest.mark.parametrize("site", tests.keys())
async def test_source_database(setup: EnvironmentForTest, site: type[GalleryDLSource]):
    """Test that we can import a post."""
    
    if site in skipped_sources:
        pytest.skip(f"Source {site} is skipped since it requires authentication")


    dataset_schema = make_default_schema("gallery-dl")
    BATCH_SIZE = 1000

    for test in tests[site].import_tests:

        output = await run_gallery_dl(setup.config, setup.local_repo, BATCH_SIZE, dataset_schema, False, test.post_url.url)

        assert output.submission_files_processed == len(test.rows), f"Expected to process {len(test.rows)} submission files, but processed {output.submission_files_processed}"
        assert output.post_metadata_files_processed == 1, f"Expected to process 1 post metadata file, but processed {output.post_metadata_files_processed}"

        async with Dataset.connect(setup.config, db_batch_size=BATCH_SIZE, dataset_schema=dataset_schema) as dataset:
            submission_rows = list(dataset.get_table("submissions").get_rows(setup.local_repo.uuid))
            metadata_rows = list(dataset.get_table("metadata").get_rows(setup.local_repo.uuid))
            assert len(submission_rows) == len(test.rows)
            assert len(metadata_rows) == 1
            for expected_row, actual_row in zip(test.rows, submission_rows):
                actual_file_key: str
                actual_source: str
                actual_id: int
                actual_metadata_key: str
                actual_part: int
                actual_file_key, actual_source, actual_id, actual_metadata_key, actual_part = actual_row
                assert FileKey.must_parse(actual_file_key.encode('utf-8')) == expected_row.file_key
                assert actual_source  == site.source_name
                assert actual_id == test.id
                assert FileKey.must_parse(actual_metadata_key.encode('utf-8')) == expected_row.metadata_file_key
                assert actual_part == expected_row.part
