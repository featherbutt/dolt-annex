#!/usr/bin/env python
# -*- coding: utf-8 -*-

import contextlib
import os
from pathlib import Path
import random
import shutil
import uuid

import pytest 

from dolt_annex.datatypes.common import Connection
from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.filestore import FileStore
from dolt_annex.filestore.base import maybe_await
from dolt_annex.filestore.cas import ContentAddressableStorage
from dolt_annex.table import Dataset
from dolt_annex.commands.import_command import ImportConfig, do_import
from dolt_annex.commands.sync.pull import pull_dataset
from dolt_annex.server.ssh import server_context as async_server_context
from dolt_annex.datatypes.remote import Repo
from dolt_annex.file_keys.sha256e import Sha256e
from tests.import_test import TestImporter

from .setup import setup, setup_ssh_remote, base_config, init

import_config = ImportConfig(
    batch_size = 10,
    follow_symlinks = False,
    file_key_type = Sha256e,
    move=False,
    copy=True,
    symlink=False,
)

import_directory = Path(__file__).parent / "import_data"
config_directory = Path(__file__).parent / "config"
remote_annex = Path(__file__).parent / "test_annex"

@pytest.mark.asyncio
async def test_pull_local(tmp_path):
    origin_uuid = uuid.uuid4()
    setup(tmp_path, origin_uuid)
    init(tmp_path)
    remote = Repo(
        url=Path(tmp_path) / "remote_files",
        uuid=origin_uuid,
        name="origin",
        key_format=Sha256e
    )
    await do_test_pull(tmp_path, "submissions", remote)

@pytest.mark.asyncio
async def test_pull_sftp(tmp_path):
    with setup_ssh_remote(tmp_path) as remote:
        await do_test_pull(tmp_path, "submissions", remote)

@pytest.mark.asyncio
async def test_pull_server(tmp_path):
    print(tmp_path)
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
    remote_file_cas = ContentAddressableStorage.from_remote(remote)
    # setup server, then create server context, then setup client.
    async with (
        remote_file_cas.open(base_config),
        async_server_context(
            remote_file_cas,
            host,
            ssh_port,
            str(Path(__file__).parent / "test_client_keys" / "id_ed25519.pub"),
            str(Path(__file__).parent / "test_client_keys" / "id_ed25519")
        ),
    ):
        init(tmp_path)
        await do_test_pull(tmp_path, "submissions", remote)

async def do_test_pull(tmp_path, dataset_name: str, remote: Repo):
    """Run and validate pulling content files from a remote"""
    tmp_path = Path(tmp_path)
    importer = TestImporter()
    remote_file_store = ContentAddressableStorage.from_remote(remote).file_store
    local_file_store = base_config.filestore
    assert local_file_store is not None
    with contextlib.chdir(config_directory):
        dataset_schema = DatasetSchema.must_load(dataset_name)
    shutil.copytree(import_directory, tmp_path / "import_data")
    async with (
        local_file_store.open(base_config),
        remote_file_store.open(base_config),
        Dataset.connect(base_config, import_config.batch_size, dataset_schema) as dataset
    ):
        await do_import(remote_file_store, remote.uuid, import_config, dataset, importer, ["import_data/00"])
        await dataset.flush()
        assert await maybe_await(remote_file_store.exists(Sha256e.from_file(import_directory / "00/12/34/56/78/591785b794601e212b260e25925636fd.e621.txt")))

        with dataset.dolt.set_branch(f"{remote.uuid}-{dataset.name}"):
            dataset.dolt.commit(amend=True)
        files_pulled = await pull_and_verify(dataset, remote, remote_file_store, local_file_store)
        assert files_pulled == 2
        # Pulling again should have no effect

        files_pulled = await pull_and_verify(dataset, remote, remote_file_store, local_file_store)
        assert files_pulled == 0


async def pull_and_verify(dataset: Dataset, file_remote: Repo, remote_file_store: FileStore, local_file_store: FileStore) -> int:

    files_pulled = await pull_dataset(dataset, base_config.get_uuid(), file_remote, remote_file_store, local_file_store, [], False)
    await dataset.flush()
        
    for key in files_pulled:
        assert local_file_store.exists(key)
    # TODO: Test that the branches are correct.

    return len(files_pulled)
