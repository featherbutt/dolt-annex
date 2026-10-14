
from pathlib import Path
import shutil
from typing import cast

from dolt_annex.constants import PROGNAME
from dolt_annex.util.prompts import Choice, choices, text, confirm, filepath # pyright: ignore[reportPrivateImportUsage]

from dolt_annex.data import data_dir
from dolt_annex.datatypes.common import MySQLConnection
from dolt_annex.datatypes.config import DoltConfig
from dolt_annex.interactive.ensure_dolt_binary import ensure_dolt_binary

import logging
logger = logging.getLogger(__name__)


async def ensure_database() -> DoltConfig:
    func = await choices(
        message=f"""{PROGNAME} stores file metadata in a Dolt database. There are several options:
        
Remote Database - {PROGNAME} will connect to a database running on another device (requires a URL beginning with mysql://). Metadata will not be stored locally, but search functionality will have access to all records on the remote.

Cloned Database - {PROGNAME} will create a local copy of an existing remote database (requires a URL beginning with https://). Metadata will be stored locally and synchronized with the remote database. Search functionality will have access to all cloned records.

Local Database - {PROGNAME} will create and use a Dolt database on the local device. Search functionality will be limited to local records. A remote database can be added later for synchronization. 
""",
        choices=[
            Choice(name="Connect to Remote Database", value=make_new_remote_database),
            Choice(name="Clone Database", value=clone_existing_database),
            Choice(name="Create New Local Database", value=make_new_local_database),
            Choice(name="Use Existing Local Database", value=use_existing_local_database),
            ]
    )
    return await func()

async def make_new_remote_database() -> DoltConfig:
    while True:
        url = await text("Enter the remote database URL (format: mysql://username@hostname:port/database_name):")
        try:
            db_conn = MySQLConnection.parse_url(url)
            break
        except ValueError as e:
            print(f"Error: {e}")
    if db_conn.password is False:
        db_conn.password = await confirm(
            message="Is a password required for the new remote database?",
            default=True
        )
    
    return DoltConfig(
        spawn_dolt_server=False,
        connection=db_conn,
    )

async def clone_existing_database() -> DoltConfig:
    dolt_binary_path = await ensure_dolt_binary()
    while True:
        url = await text("Enter the clonable database URL (either a doltlab name like user/db or a full URL like https://doltlab.hostname:port/user/db):")
        try:
            db_conn = MySQLConnection.parse_url(url)
            break
        except ValueError as e:
            print(f"Error: {e}")
    if db_conn.password is False:
        db_conn.password = await confirm(
            message="Is a password required for the new remote database?",
            default=True
        )
    
    return DoltConfig(
        spawn_dolt_server=False,
        connection=db_conn,
        dolt_binary=dolt_binary_path,
    )

async def make_new_local_database() -> DoltConfig:
    dolt_path_str = await filepath("Enter the path to where the new local Dolt database should be created: (leave empty for the current directory)")
    dolt_path = Path(dolt_path_str).expanduser() if dolt_path_str else Path.cwd()
    dolt_path = dolt_path / "dolt"
    create_dolt_directory(dolt_path)
    dolt_binary_path = await ensure_dolt_binary()
    return DoltConfig(
        spawn_dolt_server=True,
        dolt_binary=dolt_binary_path,
        dolt_dir=dolt_path,
    )

async def use_existing_local_database() -> DoltConfig:
    while True:
        dolt_path_str= await filepath("Enter the path to the Dolt database:")
        dolt_path = Path(dolt_path_str).expanduser()
        if not dolt_path.exists():
            proceed = await confirm(
                message=f"Warning: Dolt database path '{dolt_path}' does not exist. Proceed anyway?",
                default=False
            )
            if not proceed:
                continue
        break
    dolt_binary_path = await ensure_dolt_binary()
    return DoltConfig(
        spawn_dolt_server=True,
        dolt_dir=dolt_path,
        dolt_binary=dolt_binary_path,
    )

def create_dolt_directory(dolt_dir: Path):
    if not dolt_dir.exists() or not (dolt_dir / ".dolt").exists():
        print(f"Dolt directory {dolt_dir} does not exist. Creating it.")
        dolt_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(data_dir / "dolt_base" / ".dolt", dolt_dir / ".dolt")
        print(f"Initialized Dolt repository in {dolt_dir}")
