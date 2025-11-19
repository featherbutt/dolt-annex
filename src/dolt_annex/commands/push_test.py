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
from dolt_annex.file_keys.base import FileKey
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.annexfs import AnnexFS
from dolt_annex.filestore.base import ContentAddressableStorage
from dolt_annex.filestore.memory import MemoryFS

def run(*, cmd: type[cli.Application] = Application, args: Iterable[str], expected_output: Optional[str] = None):
    captured_output = StringIO()
    with contextlib.redirect_stdout(captured_output):
        cmd.run(args, exit=False)
    output = captured_output.getvalue()
    if expected_output is not None and expected_output not in output:
        raise AssertionError(f"Expected '{expected_output}' in output, got: {output}")


files: dict[FileKey, bytes] = {
    Sha256e(b"SHA256E-s5--2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824.txt"): b"hello",
    Sha256e(b"SHA256E-s5--486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7.txt"): b"world",
}

test_uuid = uuid.UUID("3fca31d9-f0dd-424e-b0e9-3cd4a26e9d68")

config_json =f"""
{{
    "uuid": "{test_uuid}",
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
            "uuid": "36b60d94-fbdf-476b-9479-f0abc61fa5ba"
        }}
    ]
}}
"""


test_file_store = MemoryFS(files=files)

async def create_test_filestore(path: Path, files: Iterable[bytes]) -> ContentAddressableStorage:
    annex_fs = AnnexFS(root=path)
    cas = ContentAddressableStorage(annex_fs, Sha256e)
    for file_content in files:
        await cas.put_file_bytes(file_content)
    return cas

config_directory = config_directory = Path(__file__).parent.parent.parent.parent / "tests" / "config"

async def setup(tmp_path: Path):
    local_filestore = await create_test_filestore(tmp_path / "local_files", [])
    remote_filestore = await create_test_filestore(tmp_path / "remote_files", files.values())
    shutil.copytree(config_directory, tmp_path, dirs_exist_ok=True)
    # TODO: Also setup Dolt
    dolt_dir = Path(tmp_path / "dolt")
    dolt_dir.mkdir()
    dolt = local.cmd.dolt.with_cwd(dolt_dir)
    dolt("init", "--name", "A U Thor", "--email", "author@example.com")
    dolt("sql", "-q", "CREATE TABLE test_table(path varchar(100) primary key, annex_key varchar(100));")
    dolt("add", ".")
    dolt("commit", "-m", "Initial commit")

    with (tmp_path / "config.json").open("w") as f:
        f.write(config_json)
    return local_filestore, remote_filestore

def test_push_local(tmp_path):
    asyncio.run(setup(tmp_path))
    do_test_push(tmp_path)

if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdirname:
        test_push_local(Path(tmpdirname))

def do_test_push(tmp_path):
    """Run and validate pushing content files to a remote"""

    with contextlib.chdir(tmp_path):
        run(
            args=["dolt-annex", "push", "--dataset", "test", "--remote", "origin"],
            expected_output="Pushed 2 files to remote 'origin'"
        )

        run(
            args=["dolt-annex", "push", "--dataset", "test", "--remote", "origin"],
            expected_output="Pushed 0 files to remote 'origin'"
        )

        # But if we add more files, it should push them
        
        run(
            args=["dolt-annex", "insert-record", "--dataset", "test", "--file-bytes", "new file content"],
        )

        run(
            args=["dolt-annex", "push", "--dataset", "test", "--remote", "origin"],
            expected_output="Pushed 1 files to remote 'origin'"
        )
