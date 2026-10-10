
from typing import cast

from InquirerPy.inquirer import select, text, confirm # pyright: ignore[reportPrivateImportUsage]
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator

from dolt_annex.datatypes.common import MySQLConnection
from dolt_annex.datatypes.config import DoltConfig
from dolt_annex.interactive.ensure_dolt_binary import ensure_dolt_binary

import logging
logger = logging.getLogger(__name__)

async def make_new_remote_database() -> DoltConfig:
    while True:
        url: str = cast(str, await text(
            message="Enter the remote database URL (format: mysql://username@hostname:port/database_name):",
        ).execute_async())
        try:
            db_conn = MySQLConnection.parse_url(url)
            break
        except ValueError as e:
            print(f"Error: {e}")
    if db_conn.password is False:
        db_conn.password = cast(bool, await confirm(
                message="Is a password required for the new remote database?",
                default=True
            ).execute_async())
    
    return DoltConfig(
        spawn_dolt_server=False,
        connection=db_conn,
    )

async def clone_existing_database() -> DoltConfig:
    ensure_dolt_binary()
    while True:
        url: str = cast(str, await text(
            message="Enter the clonable database URL (either a doltlab name like user/db or a full URL like https://doltlab.hostname:port/user/db):",
        ).execute_async())
        try:
            db_conn = MySQLConnection.parse_url(url)
            break
        except ValueError as e:
            print(f"Error: {e}")
    if db_conn.password is False:
        db_conn.password = cast(bool, await confirm(
                message="Is a password required for the new remote database?",
                default=True
            ).execute_async())
    
    return DoltConfig(
        spawn_dolt_server=False,
        connection=db_conn,
    )

async def ensure_database() -> DoltConfig:
    return await (await select(
            message=f"""dolt-annex stores file metadata in a Dolt database. There are several options:
            
    Remote Database - dolt-annex will connect to a database running on another device (requires a URL beginning with mysql://). Metadata will not be stored locally, but search functionality will have access to all records on the remote.
    
    Cloned Database - dolt-annex will create a local copy of an existing remote database (requires a URL beginning with https://). Metadata will be stored locally and synchronized with the remote database. Search functionality will have access to all cloned records.
    
    Local Database - dolt-annex will create and use a Dolt database on the local device. Search functionality will be limited to local records. A remote database can be added later for synchronization. 
    """,
            choices=[
                Choice(name="Connect to Remote Database", value=make_new_remote_database),
                Choice(name="Clone Database", value=clone_existing_database),
                Separator("Create New Local Database (Not Implemented)"),
                Separator("Use Existing Local Database (Not Implmemented)"),
                ]
        ).execute_async())() # pyright: ignore[reportOptionalCall]