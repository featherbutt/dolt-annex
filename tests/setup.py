#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
import os

from plumbum import local # type: ignore

from application import Config
from commands.init import InitConfig, do_init

config = Config(
    dolt_dir = "./dolt",
    dolt_db = "test",
    dolt_remote = "origin",
    git_dir = "./git",
    git_remote = "origin",
    email = "user@localhost",
    name = "user",
    annexcommitmessage = "commit message",
)

def setup(tmp_path):
    os.chdir(tmp_path)
    local.cmd.git("init", "--bare", "git_origin", "-b", "git-annex")
    local.cmd.git("-C", "./git_origin", "-c", "annex.tune.objecthashlower=true", "annex", "init")
    Path("./dolt_origin").mkdir()
    Path("./dolt_tmp").mkdir()
    local.cmd.dolt.with_cwd("./dolt_tmp")("init", "-b", "main")
    local.cmd.dolt.with_cwd("./dolt_tmp")("remote", "add", "origin", "file://../dolt_origin/")
    local.cmd.dolt.with_cwd("./dolt_tmp")("push", "origin", "main")
    
    init_config = InitConfig(
        init_git = True,
        init_dolt = True,
        git_url = "../git_origin",          # git_url is relative the git directory
        dolt_url = "file://./dolt_origin/", # dolt_url is relative to the base directory
        remote_name = "test_remote",
    )
    do_init(config, init_config)
