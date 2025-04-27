#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from pathlib import Path
import os
import getpass
import random

from paramiko import PKey
from plumbum import local # type: ignore

from application import Config
from commands.init import InitConfig, do_init
from commands.server_command import server_context
from db import PERSONAL_BRANCH_INIT_SQL, SHARED_BRANCH_INIT_SQL
from git import Git

base_config = Config(
    dolt_dir = "./dolt",
    dolt_db = "dolt",
    dolt_remote = "origin",
    git_dir = "./git",
    git_remote = "origin",
    email = "user@localhost",
    name = "user",
    annexcommitmessage = "commit message",
    spawn_dolt_server = True,
    auto_push = True,
    dolt_server_socket=f"/tmp/mysql{random.randint(1,1000)}.sock",
)

def setup_file_remote(tmp_path):
    setup(tmp_path)
    init("../git_origin")

@contextmanager
def setup_ssh_remote(tmp_path):
    print(tmp_path)
    user = getpass.getuser()
    # sshd_process = local.cmd.sshd.popen(["-f", "tests/config/sshd_config"], )
    # sshd_process = local.cmd.sshd["-f", "tests/config/sshd_config"] & BG
    setup(tmp_path)
    init(f"{user}@localhost:{tmp_path}/git_origin")
    yield
    # sshd_process.terminate()

def setup(tmp_path):
    os.chdir(tmp_path)
    git = local.cmd.git
    git("init", "--bare", "git_origin", "-b", "git-annex")
    git = git["-C", "./git_origin"]
    git("-c", "annex.tune.objecthashlower=true", "annex", "init")
    git_config = git["config", "--local"]
    origin_uuid = git_config("annex.uuid").strip()
    print(f"Origin UUID: {origin_uuid}")
    Path("./dolt_origin").mkdir()
    Path("./dolt_tmp").mkdir()
    dolt = local.cmd.dolt.with_cwd("./dolt_tmp")
    dolt("init", "-b", "main")
    
    dolt("checkout", "-b", origin_uuid)
    dolt("sql", "-q", PERSONAL_BRANCH_INIT_SQL)
    dolt("add", ".")
    dolt("commit", "-m", "init personal branch")
    dolt("checkout", "main")
    dolt("sql", "-q", SHARED_BRANCH_INIT_SQL)
    dolt("add", ".")
    dolt("commit", "-m", "init shared branch")
    dolt("remote", "add", "origin", "file://../dolt_origin/")
    dolt("push", "origin", "main")
    dolt("push", "origin", origin_uuid)
    
def init(git_remote_url: str):
    init_config = InitConfig(
        init_git = True,
        init_dolt = True,
        git_url = git_remote_url,          # git_url is relative the git directory
        dolt_url = "file://./dolt_origin/", # dolt_url is relative to the base directory
        remote_name = "test_remote",
    )
    do_init(base_config, init_config)
