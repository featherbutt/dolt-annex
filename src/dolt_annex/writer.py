#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from dataclasses import dataclass, field
import logging
from typing import Dict, List

from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.file_stream import KeyedFileStream
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import FileTableSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStore
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.replicated_db.interface import TableReplica

logger = logging.getLogger(__name__)

@dataclass
class CollectionTables:
    tables: List[TableReplica]

    async def insert(self, row: TableRow):
        for table in self.tables:
            await table.insert(row)

# Unlike the class in replicated_db that takes an ID and represents a single version of the DB (without the filstore)
# This class has the ability to do filestore operations
@dataclass
class ReplicaWriter:
    table: TableReplica
    filestore: Repo
    collections: CollectionTables = field(default_factory=lambda: CollectionTables(tables=[]))

    async def put_file(
        self,
        table_row: TableRow,
        data_source: KeyedFileStream,
    ) -> FileKey:
        file_key, filestore_ids = await self.filestore.put_file(data_source)
        table_row[self.table.schema.file_column] = file_key
        for filestore_id in filestore_ids:
            async with self.table.table.with_repo(filestore_id) as table_replica:
                await table_replica.insert(table_row)
        await self.collections.insert(table_row)
        return file_key

    async def insert(self, row: TableRow):
        await self.table.insert(row)
        return await self.collections.insert(row)

    @property
    def uuid(self) -> FileStore.Id:
        return self.filestore.uuid

    @property
    def schema(self) -> FileTableSchema:
        return self.table.schema
    
    _in_flight_keys: Dict[FileKey, asyncio.Future] = field(default_factory=dict)

    async def copy_row(self, from_repo: ContentAddressableStorage, table_row: TableRow, key: FileKey, ignore_missing: bool):
        logger.info("moving %s: %s", table_row, key)
        table_row[self.table.schema.file_column] = key
        async with self.table.table.with_repo(self.uuid) as table_replica:
            if await self.filestore.exists(key):
                logger.debug("file %s already exists in destination filestore", key)
                # The file may have come from a different dataset, so we don't need to copy it.
                # We still record that we have a copy of it for this dataset.
                await table_replica.insert(table_row)
                return
            if ignore_missing and not await from_repo.exists(key):
                logger.debug("Missing file %s in source filestore, skipping due to --ignore-missing", key)
                return
            if key in self._in_flight_keys:
                task = self._in_flight_keys[key]
                await task
                await table_replica.insert(table_row)
            else:
                task = asyncio.create_task(self.put_file(
                    table_row,
                    from_repo.with_file_stream(key)
                ))
                self._in_flight_keys[key] = task
                await task
                del self._in_flight_keys[key]