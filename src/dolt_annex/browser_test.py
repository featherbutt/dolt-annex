#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pathlib
import webbrowser

import pytest

from dolt_annex import browser
from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.memory import MemoryFS

class FakePrompt:
    """Stands in for InquirerPy's text prompt so tests do not block waiting for input."""
    async def execute_async(self):
        return ""

@pytest.fixture
def opened_files(monkeypatch) -> list[tuple[str, bytes]]:
    """
    Records (url, contents) for every file the browser is asked to open.

    The temporary file is deleted once display_file returns, so its contents are read at open time.
    """
    opened: list[tuple[str, bytes]] = []

    def fake_open(url: str, *args, **kwargs) -> bool:
        path = pathlib.Path(url.removeprefix("file://"))
        opened.append((url, path.read_bytes()))
        return True

    monkeypatch.setattr(browser, "text", lambda **kwargs: FakePrompt())
    monkeypatch.setattr(webbrowser, "open", fake_open)
    return opened

@pytest.mark.asyncio
async def test_display_file_opens_file_with_matching_content(opened_files: list[tuple[str, bytes]]):
    filestore = MemoryFS()
    content = b"<html>hello</html>"
    key = Sha256E.from_bytes(content, "html")
    await filestore.put_file_bytes(content, key)

    await browser.display_file(filestore, key)

    assert len(opened_files) == 1
    url, opened_contents = opened_files[0]
    assert url.endswith(".html")
    assert opened_contents == content

@pytest.mark.asyncio
async def test_display_file_without_extension(opened_files: list[tuple[str, bytes]]):
    """Keys without an extension should still display."""
    filestore = MemoryFS()
    content = b"no extension here"
    key = Sha256E.from_bytes(content)
    await filestore.put_file_bytes(content, key)

    await browser.display_file(filestore, key)

    assert len(opened_files) == 1
    assert opened_files[0][1] == content
