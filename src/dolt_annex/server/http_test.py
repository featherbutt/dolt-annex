#!/usr/bin/env python
# -*- coding: utf-8 -*-

import aiohttp
import pytest

from dolt_annex.file_keys import Sha256E
from dolt_annex.server.http import serve_filestore
from dolt_annex.filestore.memory import MemoryFS

@pytest.mark.asyncio
async def test_serve_filestore_returns_404_for_missing_key():
    filestore = MemoryFS()
    key = Sha256E.from_bytes(b"missing", "txt")

    async with serve_filestore(filestore) as http_server, aiohttp.ClientSession() as session:
        async with session.get(f"{http_server.base_url}/{key}") as response:
            assert response.status == 404

@pytest.mark.asyncio
async def test_serve_filestore_returns_400_for_invalid_key():
    filestore = MemoryFS()

    async with serve_filestore(filestore) as http_server, aiohttp.ClientSession() as session:
        async with session.get(f"{http_server.base_url}/not-a-file-key") as response:
            assert response.status == 400

@pytest.mark.asyncio
async def test_serve_filestore_stops_when_context_exits():
    filestore = MemoryFS()

    async with serve_filestore(filestore) as http_server:
        pass

    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientError):
            async with session.get(f"{http_server.base_url}/anything"):
                pass
