from typing import Sequence

from InquirerPy.inquirer import fuzzy

from dolt_annex.datatypes.common import TableRow
from dolt_annex.replicated_db.interface import TableFilter, TableReplica

async def prompt_next_column(table: TableReplica, partial_row: TableRow) -> TableRow: 
    for key_column in table.schema.key_columns:
        if key_column not in partial_row:
            next_column_values = table.get_rows(columns=[key_column], filters=TableFilter.from_table_row(partial_row))
            choices = [row[key_column] for row in next_column_values]
            if not choices:
                print(f"This repository is empty!")
                return partial_row
            choice = await fuzzy(
                message=f"Enter {key_column}:",
                choices=choices,
            ).execute_async()
            partial_row[key_column] = choice

    return partial_row