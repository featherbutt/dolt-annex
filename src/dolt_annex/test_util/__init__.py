#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections.abc import Iterable
import contextlib
from dataclasses import dataclass
import io
from pathlib import Path
import sys
from typing_extensions import Optional

from plumbum import cli # type: ignore[import]
import pytest

from dolt_annex.application import Application
from dolt_annex.datatypes.async_types import maybe_await
from dolt_annex.datatypes.config import Config, DoltConfig, UserConfig
from dolt_annex.datatypes.repo import Repo
from dolt_annex.datatypes.table import DatasetSchema, FileTableSchema
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.test_util.io_utils import BufferStringIO, TextTee, redirect_stdin

@dataclass
class EnvironmentForTest:
    """
    The output of dolt_annex.conftest.setup
    """
    local_file_store: ContentAddressableStorage
    local_repo: Repo
    remote_file_store: ContentAddressableStorage
    remote_repo: Repo

public_key_path = Path(__file__).parent / "test_keys" / "id_ed25519.pub"
private_key_path = Path(__file__).parent / "test_keys" / "id_ed25519"

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
        args: Iterable[str | bytes],
        stdin: Optional[str] = None,
        expected_output_equals: Optional[str] = None,
        expected_output_contains: Optional[str] = None,
        expected_output_does_not_contain: Optional[str] = None,
        expected_exception: Optional[type[BaseException]] = None,
        expected_error_code: int = 0
) -> None:
    """
    Run a dolt-annex CLI command and optionally check for expected output.
    
    This is designed to emulate how a user would run the command from the terminal.

    However, since the command is run in-process, things like loadable config files can be proloaded and re-used.
    """

    arg_strings: list[str] = []
    for arg in args:
        if isinstance(arg, str):
            arg_strings.append(arg)
        else:
            arg_strings.append(str(arg, encoding='utf-8'))

    with contextlib.ExitStack() as stack:

        if stdin is not None:
            stdin_io = io.StringIO(initial_value=stdin)
            stack.enter_context(redirect_stdin(stdin_io))

        captured_output = BufferStringIO()
        if (
            expected_output_contains is not None
            or expected_output_does_not_contain is not None
            or expected_output_equals is not None
        ):
            tee = TextTee(captured_output, sys.stdout)
            stack.enter_context(contextlib.redirect_stdout(tee))

        if expected_exception is not None:
            stack.enter_context(
                pytest.RaisesGroup(expected_exception, flatten_subgroups=True, allow_unwrapped=True)
            )
            
        inst, continuation = cmd.run(arg_strings, exit=False)
        error_code = await maybe_await(continuation)
        assert error_code == expected_error_code, f"Command exited with code {error_code}"

    output = captured_output.getvalue()

    if expected_output_equals is not None:
        assert output == expected_output_equals, f"Expected output:\n{expected_output_equals}\nGot:\n{output}"

    if expected_output_contains is not None:
        if expected_output_contains not in output:
            raise AssertionError(f"Expected '{expected_output_contains}' in output, got: {output}")
        
    if expected_output_does_not_contain is not None and expected_output_does_not_contain in output:
        raise AssertionError(f"Did not expect '{expected_output_does_not_contain}' in output, got: {output}")

    
