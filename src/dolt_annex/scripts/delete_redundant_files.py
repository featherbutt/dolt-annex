#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script deletes files from a dataset copy and its filestore if the files are present in one or more other filestores.
The file must be present in each of the target filestores in order to be removed.

Note that only some filestore types support deletion.
"""

import json
import logging
from typing import Dict, Iterable
from typing_extensions import Literal
from contextlib import AsyncExitStack
from plumbum import cli

from dolt_annex.commands import SubCommand
from dolt_annex.datatypes.common import TableRow
from dolt_annex.datatypes.repo import Repo, RepoModel
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.filestore.base import FileStore
from dolt_annex.replicated_db.dolt import DatabaseConnection

logger = logging.getLogger(__name__)

class DeleteRedundantFiles(SubCommand):

    delete_from = cli.SwitchAttr(
        "--delete-from",
        str,
        help="The name of the repo to delete from",
        mandatory = True,
    )

    if_in = cli.SwitchAttr(
        "--if-in",
        str,
        help="The names of the repos to check for copies in",
        list = True,
    )

    dataset = cli.SwitchAttr(
        "--dataset",
        str,
        help="The name of the dataset to delete from",
        mandatory = True,
    )

    table_name = cli.SwitchAttr(
        "--table",
        str,
        help="The name of the table to delete from",
        mandatory = True,
    )

    dry_run = cli.Flag(
        "--dry-run",
        help="If set, will only log the files that would be deleted without actually deleting them",
    )

    async def main(self, *args: str) -> Literal[0,1]:
        
        dataset_schema = DatasetSchema.must_load(self.dataset)
        
        async with AsyncExitStack() as stack:
            repo_to_delete_from = await stack.enter_async_context(Repo.open(self.config, self.delete_from))
            conn = stack.enter_context(DatabaseConnection.open(self.config))
            dataset = stack.enter_context(conn.open_dataset(dataset_schema))
            dataset_repo_to_delete_from = await stack.enter_async_context(dataset.with_repo(repo_to_delete_from.uuid))
            table_to_delete_from = dataset_repo_to_delete_from.get_table(self.table_name)
            
            row_iter: Iterable[TableRow]
            if len(args) == 0:
                row_iter = table_to_delete_from.get_rows()
            else:
                def row_generator():
                    for row_to_remove in args:
                        row_filters = json.loads(row_to_remove)
                        table_row_to_remove = table_to_delete_from.get_row(filters=row_filters)
                        if table_row_to_remove is None:
                            logger.warning(f"Row not found: {row_to_remove}")
                            continue
                        yield table_row_to_remove
                row_iter = row_generator()
                                                                    
            repos: Dict[str, Repo] = {}
            for repo_name in self.if_in:
                filestore = await stack.enter_async_context(Repo.open(self.config, repo_name))
                repos[repo_name] = filestore

            for row_to_remove in row_iter:
                file_key_string = row_to_remove.get(table_to_delete_from.schema.file_column)
                if file_key_string is None:
                    logger.fatal("Missing file key column")
                    return 1
                file_key = FileKey.must_parse(file_key_string)
                for repo_name, repo in repos.items():
                    # TODO: exists and verify require multiple round trip times. Make a combined function that only requires a single probe
                    if not repo.filestore.file_store.exists(file_key):
                        logger.info(f"key {file_key} does not exist on repo {repo_name}")
                        can_remove = False
                        break
                    await repo.filestore.verify_file(file_key)
                else:
                    can_remove = True
                if can_remove:
                    if not self.dry_run:
                        repo_to_delete_from.filestore.file_store.delete(file_key)
                        await table_to_delete_from.remove(row_to_remove)
                    else:
                        logger.info(f"Would delete file: {file_key}")

        return 0

Command = DeleteRedundantFiles
