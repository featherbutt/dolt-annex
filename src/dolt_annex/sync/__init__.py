#!/usr/bin/env python
# -*- coding: utf-8 -*-

from uuid import UUID
from dataclasses import dataclass, field
from typing_extensions import Iterable, Optional, Tuple, List, Any

from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.base import maybe_await
from dolt_annex.table import FileTable
from dolt_annex.logger import logger
from dolt_annex.datatypes import TableRow
from dolt_annex.dolt import DoltSqlServer
from dolt_annex.datatypes.table import FileTableSchema

@dataclass
class TableFilter:
    column_name: str
    column_value: Any

@dataclass
class SyncResults:
    files_pushed: List[FileKey] = field(default_factory=list)
    files_pulled: List[FileKey] = field(default_factory=list)

    def __iadd__(self, other: 'SyncResults') -> 'SyncResults':
        self.files_pushed += other.files_pushed
        self.files_pulled += other.files_pulled
        return self
    
    def __bool__(self) -> bool:
        return bool(self.files_pushed or self.files_pulled)
    
class FileModifiedError(Exception):
    def __init__(self, key: FileKey):
        self.key = key
        super().__init__(f"File with annex key {key} has different content on both remotes")

async def move_table(table: FileTable, from_uuid: UUID, to_uuid: UUID, from_file_store: FileStore, to_file_store: FileStore, where: List[TableFilter], limit: Optional[int] = None, out_moved_keys: Optional[List[FileKey]] = None) -> List[FileKey]:
    if out_moved_keys is None:
        out_moved_keys = []
    dolt = table.dolt

    while True:
        keys_and_submissions = list(diff_keys(dolt, str(from_uuid), str(to_uuid), table.dataset_name, table.schema, where, limit))
        has_more = await move_submissions_and_keys(keys_and_submissions, table, from_file_store, to_file_store, to_uuid, out_moved_keys)
        if not has_more:
            break
    return out_moved_keys

async def move_submissions_and_keys(keys_and_submissions: Iterable[Tuple[FileKey, TableRow]], file_table: FileTable, from_file_store: FileStore, to_file_store: FileStore, destination_uuid: UUID, files_moved: List[FileKey]) -> bool:
    has_more = False
    for key, table_row in keys_and_submissions:
        has_more = True
        logger.info(f"moving {table_row}: {key}")

        async with from_file_store.with_file_object(key) as remote_file_obj:
            await maybe_await(to_file_store.put_file_object(remote_file_obj, key))

        await file_table.insert_file_source(table_row, key, destination_uuid)
        files_moved.append(key)
    await file_table.flush()
    return has_more

def diff_keys(dolt: DoltSqlServer, in_ref: str, not_in_ref: str, dataset_name: str, file_key_table: FileTableSchema, filters: List[TableFilter], limit = None) -> Iterable[Tuple[FileKey, TableRow]]:
    refs = [in_ref, not_in_ref]
    refs.sort()
    union_branch_name = f"union-{refs[0]}-{refs[1]}-{dataset_name}"
    
    in_ref_branch = f"{in_ref}-{dataset_name}"
    not_in_ref_branch = f"{not_in_ref}-{dataset_name}"
    # Create the union branch if it doesn't exist
    with dolt.maybe_create_branch(union_branch_name, in_ref_branch):
        dolt.merge(in_ref_branch)
        dolt.merge(not_in_ref_branch)
        query = diff_query(file_key_table, filters)
        if limit is not None:
            query += " LIMIT %s"
            query_results = dolt.query(query, (not_in_ref_branch, union_branch_name, limit))
        else:
            query_results = dolt.query(query, (not_in_ref_branch, union_branch_name))
        # TODO: Wrap this in a helper function
        for (annex_key, _, *key_parts) in query_results:
            yield (FileKey(bytes(annex_key, encoding='utf-8')), TableRow(tuple(key_parts)))

def diff_query(file_key_table: FileTableSchema, filters: List[TableFilter]) -> str:
    """
    Generates a SQL query to identify the files that exist on one remote but not another.
    Note that generating a SQL query this way is not safe from SQL injection, but SQL injection
    isn't part of the threat model, since any query that the application can run,
    the user can already run themselves.
    """
    return f"""
        SELECT
            to_{file_key_table.file_column}, `diff_type`, {",".join("to_" + col for col in file_key_table.key_columns)}
        FROM dolt_commit_diff_{file_key_table.name}
        WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s)
        {''.join(f" AND to_{f.column_name} = %s" for f in filters)}
        """

