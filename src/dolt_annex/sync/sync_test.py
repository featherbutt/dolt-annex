#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
import pytest
import tempfile
import random

import fs.osfs

from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.repo import Repo
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.table import Dataset
from dolt_annex.sync import move_dataset
from dolt_annex.test_util import EnvironmentForTest, test_config, test_dataset_schema

@pytest.mark.asyncio
async def test_async_move(setup: EnvironmentForTest):
    with (
        tempfile.TemporaryDirectory() as from_dir, 
        tempfile.TemporaryDirectory() as to_dir,
    ):
        from_filestore = AnnexFS(fs.osfs.OSFS(from_dir))
        from_cas = ContentAddressableStorage(
            from_filestore,
            file_key_format=Sha256e,
        )
        from_uuid = uuid.uuid4()
        from_repo = Repo(
            "from_repo",
            from_uuid,
            from_filestore,
            Sha256e,
        )
        to_filestore = AnnexFS(fs.osfs.OSFS(to_dir))
        to_uuid = uuid.uuid4()
        to_repo = Repo(
            "to_repo",
            to_uuid,
            to_filestore,
            Sha256e,
        )
        # Create random files to move in parallel
        NUM_FILES = 20
        file_keys = []
        for _ in range(NUM_FILES):
            file_bytes = random.randbytes(1024**2) # 1 MB
            file_key = await from_cas.put_file_bytes(file_bytes)
            file_keys.append(file_key)

        BATCH_SIZE = 1000 # Arbitrary batch size for this command
        FILTERS = []
        async with Dataset.connect(test_config, BATCH_SIZE, test_dataset_schema) as dataset:
            # TODO: handle initializing branches automatically
            dataset.dolt.initialize_dataset_source(test_dataset_schema, from_repo.uuid)
            dataset.dolt.initialize_dataset_source(test_dataset_schema, to_repo.uuid)
            # Add entries to from_repo database
            table = dataset.get_table("test_table")
            for i, file_key in enumerate(file_keys):
                path = f"{i}"
                await table.insert_file_source(
                    TableRow((path,)),
                    file_key,
                    from_repo.uuid,
                )

            await table.flush()
            await move_dataset(
                dataset,
                from_repo,
                to_repo,
                FILTERS,
            )
            # Check that files have been moved
            for file_key in file_keys:
                assert to_filestore.exists(file_key)
                assert await to_filestore.get_file_bytes(file_key) == await from_filestore.get_file_bytes(file_key)
                # Check that db entries have been updated
                for file_key, path in table.get_rows(to_repo.uuid, FILTERS):
                    assert bytes(file_keys[int(path)]) == bytes(file_key, encoding='utf-8')

                