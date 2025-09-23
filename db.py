#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Code for interacting directly with SQL"""

from uuid import UUID

from typing_extensions import Optional

from dolt import DoltSqlServer
from tables import FileKeyTable
from type_hints import TableRow

def get_annex_key_from_submission_id(dolt: DoltSqlServer, row: TableRow, uuid: UUID, table: FileKeyTable) -> Optional[str]:
    '''Get the annex key for a given submission ID'''
    res = dolt.query(f"SELECT `{table.file_column}` FROM `{dolt.db_name}/{uuid}-{table.name}`.{table.name} WHERE {' AND '.join(f'`{col}` = %s' for col in table.key_columns)}",
                   row)
    for row in res:
        return row[0]
    return None
