
from pathlib import Path
import shutil

from plumbum import local, FG # pyright: ignore[reportAttributeAccessIssue]

from dolt_annex.constants import PROGNAME
from dolt_annex.util.prompts import Choice, choices, text, confirm, filepath # pyright: ignore[reportPrivateImportUsage]

from dolt_annex.data import data_dir
from dolt_annex.config.base import DoltConfig
from dolt_annex.config.connection import HTTPSConnection, MySQLConnection
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
        url = await text("Enter the clonable database URL (either a DoltHub name like user/db or a full URL like https://doltlab.hostname:port/user/db or https://www.dolthub.com/repositories/user/db):")
        if url.count("/") == 1 and not url.startswith("http"):
            url = f"https://www.dolthub.com/repositories/{url}"
        try:
            https_connection = HTTPSConnection.parse_url(url)
            break
        except ValueError as e:
            print(f"Error: {e}")

    await ensure_dolt_login(dolt_binary_path, https_connection)

    dolt_dir = Path.cwd() / "dolt"
    if dolt_dir.exists():
        raise FileExistsError(f"Cannot clone into {dolt_dir} because it already exists.")
    print(f"Cloning {url} into {dolt_dir}...")
    local[dolt_binary_path.as_posix()].run(["clone", url, dolt_dir.as_posix()])
    print(f"Cloned database into {dolt_dir}")

    return DoltConfig(
        spawn_dolt_server=True,
        dolt_binary=dolt_binary_path,
        dolt_dir=dolt_dir,
    )

async def ensure_dolt_login(dolt_binary_path: Path, https_connection: HTTPSConnection):
    do_login = await confirm(
        message=f"Do you want to log in to {https_connection.hostname}? This will be necessary to push database updates.",
        default=True
    )
    args = ["login"]
    if "dolthub" not in https_connection.hostname:
        args.append("--insecure")
        args.extend(["--auth-endpoint", f"{https_connection.hostname}:{https_connection.port}"])
        args.extend(["--login-url", f"https://{https_connection.hostname}/settings/credentials"])
    if do_login:
        local[dolt_binary_path.as_posix()][args] & FG


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
