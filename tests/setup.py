#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from pathlib import Path
import os
import getpass
import uuid
from uuid import UUID

from plumbum import local

from dolt_annex.commands.init import InitConfig, do_init
from dolt_annex.datatypes import remote
from dolt_annex.datatypes.config import Config, DoltConfig, SshSettings
from dolt_annex.datatypes.remote import Repo
from dolt_annex.datatypes.common import Connection
from dolt_annex.filestore.annexfs import AnnexFS

local_filestore = AnnexFS(root=Path("./files"))

base_config = Config(
    dolt=DoltConfig(
        db_name="dolt",
        spawn_dolt_server=True,
        default_remote="origin",
        dolt_dir=Path("./dolt"),
    ),
    filestore=local_filestore,
    ssh=SshSettings(
        ssh_config=Path(__file__).parent / "config" / "ssh_config",
        #known_hosts=str(Path(__file__).parent / "test_client_keys" / "known_hosts"),
        encrypted_ssh_key=False,
        client_key=Path(__file__).parent / "test_client_keys" / "id_ed25519",
    ),
    uuid=uuid.uuid4(),
)

def setup_file_remote(tmp_path):
    origin_uuid = uuid.uuid4()
    setup(tmp_path, origin_uuid)
    init()
    Path(os.path.join(tmp_path, "remote_files")).mkdir()
    return Repo.model_validate({
        "url": f"{tmp_path}/remote_files",
        "uuid": origin_uuid,
        "name": "origin",
        "key_format": "Sha256e"
    })

@contextmanager
def setup_ssh_remote(tmp_path):
    user = getpass.getuser()
    # sshd_process = local.cmd.sshd.popen(["-f", "tests/config/sshd_config"], )
    # sshd_process = local.cmd.sshd["-f", "tests/config/sshd_config"] & BG
    origin_uuid = uuid.uuid4()
    setup(tmp_path, origin_uuid)
    init()
    Path(os.path.join(tmp_path, "remote_files")).mkdir()
    yield Repo.model_validate({
        "url": Connection(
            user=user,
            host="localhost",
            port=22,
            path=f"{tmp_path}/remote_files"
        ),
        "uuid": origin_uuid,
        "name": "origin",
        "key_format": "Sha256e"
    })
    # sshd_process.terminate()

def setup(tmp_path, origin_uuid: UUID):
    os.chdir(tmp_path)
    print(f"Origin UUID: {origin_uuid}")
    Path("./dolt_origin").mkdir()
    # Dolt remotes are slightly different than local repos, so we make a temp repo and push it.
    Path("./dolt_tmp").mkdir()
    dolt = local.cmd.dolt.with_cwd("./dolt_tmp")
    dolt("init")

    dolt("branch", "urls")
    dolt("checkout", "-b", "submissions")
    dolt("sql", "-q", "CREATE TABLE IF NOT EXISTS `submissions` ( source VARCHAR(100), id int, updated DATETIME, part int, annex_key VARCHAR(100), PRIMARY KEY(source, id, updated, part) )")
    dolt("add", ".")
    dolt("commit", "-m", "init submissions table")
    dolt("checkout", "-b", f"{origin_uuid}-submissions")
    dolt("checkout", "urls")   
    dolt("sql", "-q", "CREATE TABLE IF NOT EXISTS `urls` ( url VARCHAR(1000) primary key, annex_key VARCHAR(100))")
    dolt("add", ".")
    dolt("commit", "-m", "init urls table")
    dolt("checkout", "-b", f"{origin_uuid}-urls")
    dolt("remote", "add", "origin", "file://../dolt_origin/")
    dolt("push", "origin", "submissions")
    dolt("push", "origin", "urls")
    dolt("push", "origin", f"{origin_uuid}-submissions")
    dolt("push", "origin", f"{origin_uuid}-urls")

def init():
    init_config = InitConfig(
        init_dolt = True,
        dolt_url = "file://../dolt_origin/", 
        remote_name = "origin",
    )
    Path(base_config.dolt.db_name).mkdir(parents=True, exist_ok=True)
    do_init(base_config, init_config)
