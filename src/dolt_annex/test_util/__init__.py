#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections.abc import Iterable
import contextlib
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
import sys
import uuid
from typing_extensions import Optional

from plumbum import cli # type: ignore[import]
import pytest

from dolt_annex.application import Application
from dolt_annex.datatypes.async_utils import maybe_await
from dolt_annex.datatypes.config import Config, DoltConfig, UserConfig
from dolt_annex.datatypes.repo import RepoModel
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.filestore.memory import MemoryFS, MemoryFSModel
from dolt_annex.test_util.io import Tee

@dataclass
class EnvironmentForTest:
    """
    The output of dolt_annex.conftest.setup
    """
    local_file_store: ContentAddressableStorage
    remote_file_store: ContentAddressableStorage

public_key_path = Path(__file__).parent / "test_keys" / "id_ed25519.pub"
private_key_path = Path(__file__).parent / "test_keys" / "id_ed25519"

# Arbitrary UUIDs for local and remote repos
local_uuid = uuid.UUID("3fca31d9-f0dd-424e-b0e9-3cd4a26e9d68")
remote_uuid = uuid.UUID("36b60d94-fbdf-476b-9479-f0abc61fa5ba")

test_config = Config(
    user=UserConfig(
        name="A U Thor",
        email="author@example.com"
    ),
    dolt=DoltConfig(
        default_remote="origin",
        default_commit_message="update",
        spawn_dolt_server=True
    ),
    default_file_key_type=Sha256e,
)

test_dataset_schema = DatasetSchema(
    name="test",
    tables= [
        FileTableSchema(
            name="test_table",
            file_column="annex_key",
            key_columns=["path"]
        )
    ],
    empty_table_ref= "test_dataset"
)

async def run(
        *,
        cmd: type[cli.Application] = Application,
        args: Iterable[str],
        expected_output: Optional[str] = None,
        expected_output_does_not_contain: Optional[str] = None,
        expected_exception: Optional[type[Exception]] = None,
        expected_error_code: int = 0
) -> None:
    """
    Run a dolt-annex CLI command and optionally check for expected output.
    
    This is designed to emulate how a user would run the command from the terminal.

    However, since the command is run in-process, things like loadable config files can be proloaded and re-used.
    """
    async def inner():
        with pytest.raises(expected_exception) if expected_exception is not None else contextlib.nullcontext():
            inst, continuation = cmd.run(args, exit=False)
            error_code = await maybe_await(continuation)
            assert error_code == expected_error_code, f"Command exited with code {error_code}"
    if expected_output is not None or expected_output_does_not_contain is not None:
        captured_output = StringIO()
        tee = Tee(captured_output, sys.stdout)
        with contextlib.redirect_stdout(tee):
            await inner()
        output = captured_output.getvalue()
        if expected_output is not None and expected_output not in output:
            raise AssertionError(f"Expected '{expected_output}' in output, got: {output}")
        if expected_output_does_not_contain is not None and expected_output_does_not_contain in output:
            raise AssertionError(f"Did not expect '{expected_output_does_not_contain}' in output, got: {output}")
    else:
        await inner()
    
async def create_test_filestore(name: str, uuid: uuid.UUID, files: Iterable[bytes]) -> ContentAddressableStorage:
    annex_fs_model = MemoryFSModel()
    repo = RepoModel(
        name=name,
        uuid=uuid,
        key_format=Sha256e,
        filestore= annex_fs_model
    )
    annex_fs = MemoryFS(files=annex_fs_model.files)
    cas = ContentAddressableStorage(annex_fs, Sha256e)
    for file_content in files:
        await cas.put_file_bytes(file_content)
    return cas
