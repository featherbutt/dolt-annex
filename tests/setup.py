#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
import os

from plumbum import local # type: ignore

from application import Config
from commands.init import InitConfig, do_init
from db import SHARED_BRANCH_INIT_SQL

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
)

def setup(tmp_path):
    os.chdir(tmp_path)
    git = local.cmd.git
    git("init", "--bare", "git_origin", "-b", "git-annex")
    git = git["-C", "./git_origin"]
    git("-c", "annex.tune.objecthashlower=true", "annex", "init")
    git_config = git["config", "--local"]
    local_uuid = git_config("annex.uuid").strip()
    Path("./dolt_origin").mkdir()
    Path("./dolt_tmp").mkdir()
    dolt = local.cmd.dolt.with_cwd("./dolt_tmp")
    dolt("init", "-b", "main")
    
    dolt("sql", "-q", SHARED_BRANCH_INIT_SQL)
    dolt("add", ".")
    dolt("commit", "-m", "init shared branch")
    dolt("remote", "add", "origin", "file://../dolt_origin/")
    dolt("push", "origin", "main")
    
    init_config = InitConfig(
        init_git = True,
        init_dolt = True,
        git_url = "../git_origin",          # git_url is relative the git directory
        dolt_url = "file://./dolt_origin/", # dolt_url is relative to the base directory
        remote_name = "test_remote",
    )
    do_init(base_config, init_config)
