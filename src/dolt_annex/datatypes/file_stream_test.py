#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio

import pytest

from dolt_annex.datatypes.errors import ContentAddressableStorageError
from dolt_annex.datatypes.file_io import async_bytes_io
from dolt_annex.datatypes.file_stream import (
    bytes_file_source,
    fork_readable_stream,
    known_key_source,
    known_key_source_nocheck,
)
from dolt_annex.file_keys import Sha256E

@pytest.mark.asyncio
async def test_bytes_file_source():
    data = b"some file content"
    stream = bytes_file_source(data, Sha256E, extension="txt")

    calls = 0
    produce = stream.file_key_producer
    async def counting_producer():
        nonlocal calls
        calls += 1
        return await produce()

    stream.file_key_producer = counting_producer
    
    async with stream as src:
        assert await src.read() == data

    # Assert that the underlying file key producer is awaited exactly once and the result is cached.
    assert await stream.file_key() == Sha256E.from_bytes(data, "txt")
    assert await stream.file_key() == Sha256E.from_bytes(data, "txt")
    assert calls == 1
    

@pytest.mark.asyncio
async def test_known_key_source():
    data = b"matching content"
    expected_key = Sha256E.from_bytes(data, "txt")
    stream = known_key_source(async_bytes_io(data), expected_key)

    async with stream as src:
        assert await src.read() == data

    assert await stream.file_key() == expected_key

@pytest.mark.asyncio
async def test_known_key_source_mismatched_content():
    expected_key = Sha256E.from_bytes(b"expected content", "txt")
    stream = known_key_source(async_bytes_io(b"different content"), expected_key)

    async with stream as src:
        await src.read()

    with pytest.raises(ContentAddressableStorageError):
        await stream.file_key()

@pytest.mark.asyncio
async def test_known_key_source_nocheck_does_not_verify_content():
    """nocheck variant trusts the provided key, even if the content does not match it."""
    claimed_key = Sha256E.from_bytes(b"claimed", "txt")
    stream = known_key_source_nocheck(async_bytes_io(b"actual"), claimed_key)

    async with stream as src:
        assert await src.read() == b"actual"

    assert await stream.file_key() == claimed_key

@pytest.mark.asyncio
async def test_fork_readable_stream():
    data = b"0123456789" * 1000
    source = bytes_file_source(data, Sha256E, extension="txt")
    forks = fork_readable_stream(source, num_forks=3, max_buffer_size=256)

    async def read_fork(fork):
        async with fork as src:
            return await src.read()

    results = await asyncio.gather(*(read_fork(fork) for fork in forks))

    assert results == [data, data, data]

    expected_key = Sha256E.from_bytes(data, "txt")
    for fork in forks:
        assert await fork.file_key() == expected_key

@pytest.mark.asyncio
async def test_fork_readable_stream_empty_source():
    source = bytes_file_source(b"", Sha256E, extension="txt")
    forks = fork_readable_stream(source, num_forks=2)

    async def read_fork(fork):
        async with fork as src:
            return await src.read()

    results = await asyncio.gather(*(read_fork(fork) for fork in forks))

    assert results == [b"", b""]
    assert await forks[0].file_key() == Sha256E.from_bytes(b"", "txt")
