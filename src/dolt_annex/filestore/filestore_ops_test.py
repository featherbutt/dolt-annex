#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID

import pytest

from dolt_annex.datatypes.file_stream import bytes_file_source
from dolt_annex.datatypes.filestore_config import FilestoreConfig
from dolt_annex.file_keys import Sha256E
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.memory import MemoryFS
from dolt_annex.filestore.unionfs import UnionFS

PARENT_UUID = UUID("00000000-0000-4000-8000-000000000001")
CHILD_A_UUID = UUID("00000000-0000-4000-8000-00000000000a")
CHILD_B_UUID = UUID("00000000-0000-4000-8000-00000000000b")

@pytest.mark.asyncio
async def test_put_file_object_reports_filestore_uuid():
    fs = MemoryFS(uuid=CHILD_A_UUID)
    data = b"single filestore content"

    result = await fs.put_file_object(bytes_file_source(data, Sha256E, extension="txt"))

    assert result.uuids == [CHILD_A_UUID]

@pytest.mark.asyncio
async def test_unionfs_write_to_all_children():
    union = UnionFS(
        uuid=PARENT_UUID,
        children=[MemoryFS(uuid=CHILD_A_UUID), MemoryFS(uuid=CHILD_B_UUID)],
        write_to_all_children=True,
    )
    data = b"written to both children"

    result = await union.put_file_object(bytes_file_source(data, Sha256E, extension="txt"))

    assert set(result.uuids) == {CHILD_A_UUID, CHILD_B_UUID, PARENT_UUID}

@pytest.mark.asyncio
async def test_unionfs_write_to_first_child():
    union = UnionFS(
        uuid=PARENT_UUID,
        children=[MemoryFS(uuid=CHILD_A_UUID), MemoryFS(uuid=CHILD_B_UUID)],
        write_to_all_children=False,
    )
    data = b"written to first child only"

    result = await union.put_file_object(bytes_file_source(data, Sha256E, extension="txt"))

    assert set(result.uuids) == {CHILD_A_UUID, PARENT_UUID}

@pytest.mark.asyncio
async def test_cas_put_file_propagates_uuids_from_every_modified_child():
    """
    ContentAddressableStorage.put_file should report every filestore that was modified,
    not just the top-level filestore, so that callers can record where the content lives.
    """
    union = UnionFS(
        uuid=None,
        children=[MemoryFS(uuid=CHILD_A_UUID), MemoryFS(uuid=CHILD_B_UUID)],
        write_to_all_children=True,
    )
    cas = ContentAddressableStorage(FilestoreConfig(), union, [Sha256E])
    data = b"cas content in both children"

    result = await cas.put_file(bytes_file_source(data, Sha256E, extension="txt"))

    assert set(result.uuids) == {CHILD_A_UUID, CHILD_B_UUID}
