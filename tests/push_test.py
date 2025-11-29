#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import os
from pathlib import Path
import random
import shutil
from typing import Optional
import uuid

import asyncssh
import pytest

from dolt_annex.datatypes.common import Connection
from dolt_annex.datatypes.remote import Repo
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.file_keys.sha256e import Sha256e
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.base import maybe_await
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.server.ssh import server_context as async_server_context
from dolt_annex.table import Dataset
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.sync.push import push_dataset


from tests.import_test import TestImporter
from tests.setup import setup, setup_file_remote, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    follow_symlinks = False,
    file_key_type=Sha256e,
    move=False,
    copy=True,
    symlink=False,
)

import_directory = os.path.join(os.path.dirname(__file__), "import_data")
config_directory = Path(__file__).parent / "config"

@pytest.mark.asyncio
async def test_push_local(tmp_path):
    remote = setup_file_remote(tmp_path)
    await do_test_push(tmp_path, "submissions", remote)

@pytest.mark.asyncio
async def test_push_sftp(tmp_path):
    with setup_ssh_remote(tmp_path) as remote:
        await do_test_push(tmp_path, "submissions", remote)

@pytest.mark.asyncio
async def test_push_server(tmp_path: Path):
    os.chdir(tmp_path)
    origin_uuid = uuid.uuid4()
    
    Path(os.path.join(tmp_path, "remote_files")).mkdir()
    file_remote = Repo(
        url=Path(tmp_path) / "remote_files",
        uuid=origin_uuid,
        name="origin",
        key_format=Sha256e
    )

    host = "localhost"
    ssh_port = random.randint(21000, 22000)
    setup(tmp_path, origin_uuid)
    remote = Repo(
        url=Connection(
            host=host,
            port=ssh_port,
            path=tmp_path,
            client_key=Path(__file__).parent / "test_client_keys" / "id_ed25519"
        ),
        uuid=origin_uuid,
        name="origin",
        key_format=Sha256e
    )
    remote_file_cas = ContentAddressableStorage.from_remote(file_remote)

    # setup server, then create server context, then setup client.
    #with local_server_context(remote_file_cas, base_config, str(Path(__file__).parent / "test_client_keys")) as client:
    async with async_server_context(
        remote_file_cas,
        host,
        ssh_port,
        str(Path(__file__).parent / "test_client_keys" / "id_ed25519.pub"),
        str(Path(__file__).parent / "test_client_keys" / "id_ed25519")
    ):
        #with server_context(cas, host, ssh_port, server_key, Path("~/.ssh").expanduser().as_posix()):

        init()
        await do_test_push(tmp_path, "submissions", remote)

async def do_test_push(tmp_path, dataset_name: str, remote: Repo, client: Optional[asyncssh.SFTPClient] = None):
    """Run and validate pushing content files to a remote"""
    tmp_path = Path(tmp_path)
    importer = TestImporter()
    shutil.copy(config_directory / "submissions.dataset", tmp_path / "submissions.dataset")
    shutil.copy(config_directory / "urls.dataset", tmp_path / "urls.dataset")
    shutil.copytree(import_directory, tmp_path / "import_data")

    importer = TestImporter()
    remote_file_store = ContentAddressableStorage.from_remote(remote).file_store
    if client:
        remote_file_store._sftp = client
    local_file_store = base_config.get_filestore()
    local_uuid = base_config.get_uuid()
    with contextlib.chdir(config_directory):
        dataset_schema = DatasetSchema.must_load(dataset_name)
    
    async with (
        local_file_store.open(base_config),
        remote_file_store.open(base_config),
        Dataset.connect(base_config, import_config.batch_size, dataset_schema) as dataset,
    ):
            await do_import(local_file_store, local_uuid, import_config, dataset, importer, ["import_data/00"])
            await dataset.flush()
            with dataset.dolt.set_branch(f"{local_uuid}-{dataset_name}"):
                dataset.dolt.commit(amend=True)

            files_pushed = await push_and_verify(dataset, remote, local_uuid, remote_file_store, local_file_store)
            assert files_pushed == 2
            # Pushing again should have no effect

            files_pushed = await push_and_verify(dataset, remote, local_uuid, remote_file_store, local_file_store)
            assert files_pushed == 0

            # But if we add more files, it should push them
            await do_import(local_file_store, local_uuid, import_config, dataset, importer, ["import_data/08"])
            await dataset.flush()
            with dataset.dolt.set_branch(f"{local_uuid}-{dataset_name}"):
                dataset.dolt.commit(amend=True)

            files_pushed = await push_and_verify(dataset, remote, local_uuid, remote_file_store, local_file_store)
            assert files_pushed == 1


async def push_and_verify(dataset: Dataset, file_remote: Repo, local_uuid: uuid.UUID, remote_file_store: FileStore, local_file_store: FileStore):
    files_pushed = await push_dataset(dataset, local_uuid, file_remote, remote_file_store, local_file_store, [], limit=None)
    await dataset.flush()

    for key in files_pushed:
        assert await maybe_await(remote_file_store.exists(key)), f"Remote filestore missing pushed file {key}"
    # TODO: Test that the branches are correct.

    return len(files_pushed)
