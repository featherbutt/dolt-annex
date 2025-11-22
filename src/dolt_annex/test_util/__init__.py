#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from collections.abc import Iterable
import contextlib
from io import StringIO
from pathlib import Path
import shutil
from typing import Optional
import uuid

from plumbum import local, cli

from dolt_annex.application import Application
from dolt_annex.datatypes.config import Config
from dolt_annex.datatypes.remote import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.file_keys.base import FileKey
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.memory import MemoryFS
from dolt_annex.table import Dataset

# Arbitrary UUIDs for local and remote repos
local_uuid = uuid.UUID("3fca31d9-f0dd-424e-b0e9-3cd4a26e9d68")
remote_uuid = uuid.UUID("36b60d94-fbdf-476b-9479-f0abc61fa5ba")

config_json =f"""
{{
    "uuid": "{local_uuid}",
    "user": {{
        "name": "A U Thor",
        "email": "author@example.com"
    }},

    "dolt": {{
        "db_name": "dolt",
        "default_remote": "origin",
        "default_commit_message": "update",

        "connection": {{
            "user": "root"
        }},
        "spawn_dolt_server": true
    }},
    
    "file_key_format": "Sha256e",

    "filestore": {{
        "type": "AnnexFS",
        "root": "./local_files"
    }},

    "remotes": [
        {{
            "name": "origin",
            "url": "../local_remote",
            "uuid": "{remote_uuid}"
        }}
    ]
}}
"""
test_config = Config.model_validate_json(config_json)

test_remote = Repo(name="test_remote", uuid=remote_uuid, url=Path("../remote_files"), key_format=Sha256e)

test_dataset_schema = DatasetSchema(
    name="test",
    tables= [
        FileTableSchema(
            name="test_table",
            file_column="annex_key",
            key_columns=["path"]
        )
    ],
    empty_table_ref= "main"
)

def run(*, cmd: type[cli.Application] = Application, args: Iterable[str], expected_output: Optional[str] = None):
    """
    Run a dolt-annex CLI command and optionally check for expected output.
    
    This is designed to emulate how a user would run the command from the terminal.

    However, since the command is run in-process, things like loadable config files can be proloaded and re-used.
    """
    captured_output = StringIO()
    with contextlib.redirect_stdout(captured_output):
        cmd.run(args, exit=False)
    output = captured_output.getvalue()
    if expected_output is not None and expected_output not in output:
        raise AssertionError(f"Expected '{expected_output}' in output, got: {output}")

async def create_test_filestore(path: Path, files: Iterable[bytes]) -> ContentAddressableStorage:
    annex_fs = AnnexFS(root=path)
    cas = ContentAddressableStorage(annex_fs, Sha256e)
    for file_content in files:
        await cas.put_file_bytes(file_content)
    return cas

async def setup(tmp_path: Path, local_files: Optional[Iterable[bytes]] = None, remote_files: Optional[Iterable[bytes]] = None) -> tuple[ContentAddressableStorage, ContentAddressableStorage]:
    local_filestore = await create_test_filestore(tmp_path / "local_files", local_files or [])
    remote_filestore = await create_test_filestore(tmp_path / "remote_files", remote_files or [])
    
    setup_dolt(tmp_path)

    with (tmp_path / "config.json").open("w") as f:
        f.write(config_json)
    return local_filestore, remote_filestore

def setup_dolt(tmp_path):
    dolt_dir = Path(tmp_path / "dolt")
    dolt_dir.mkdir()
    dolt = local.cmd.dolt.with_cwd(dolt_dir)
    dolt("init", "--name", "A U Thor", "--email", "author@example.com")
    dolt("sql", "-q", "CREATE TABLE test_table(path varchar(100) primary key, annex_key varchar(100));")
    dolt("add", ".")
    dolt("commit", "-m", "Initial commit")