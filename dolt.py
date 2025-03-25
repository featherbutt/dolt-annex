#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
import time
from typing import Any, Dict, Tuple

from plumbum import local # type: ignore
import pymysql

from dry_run import dry_run
from logger import logger

class DoltSqlServer:

    db_config: Dict[str, Any]
    connection: pymysql.connections.Connection
    cursor: pymysql.cursors.Cursor
    active_branch: str

    def __init__(self, dolt_dir: str, db_config: Dict[str, Any], spawn_dolt_server: bool):
        self.db_config = db_config

        if spawn_dolt_server:
            self.dolt_server_process, self.connection = self.spawn_dolt_server(dolt_dir)
        else:
            self.dolt_server_process = None
            self.connection = pymysql.connect(**db_config)

        self.cursor = self.connection.cursor()
        self.garbage_collect()

        self.cursor.execute("SELECT ACTIVE_BRANCH()")
        res = self.cursor.fetchone()
        assert res is not None
        self.active_branch = res[0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.dolt_server_process:
            self.dolt_server_process.terminate()

    def spawn_dolt_server(self, dolt_dir: str) -> Tuple[Any, pymysql.connections.Connection]:
        dolt = local.cmd.dolt.with_cwd(dolt_dir)
        dolt_server_process = dolt.popen("sql-server")
        while True:
            try:
                return dolt_server_process, pymysql.connect(**self.db_config)
            except Exception as e:
                logger.info(f"Waiting for SQL server: {str(e)}")
                time.sleep(1)

    @dry_run("Would run garbage collection")
    def garbage_collect(self):
        try:
            self.cursor.execute("call DOLT_GC();")
        except pymysql.err.OperationalError as e:
            if "no changes since last gc" not in str(e):
                raise
        self.connection = pymysql.connect(**self.db_config)
        self.cursor = self.connection.cursor()

    @dry_run("Would execute {sql} with values {values}")
    def executemany(self, sql: str, values):
        self.cursor.executemany(sql, values)
        self.cursor.execute("COMMIT;")
        self.connection.commit()

    @dry_run("Would execute {sql} with values {values}")
    def execute(self, sql: str, values):
        self.cursor.execute(sql, values)
        res = self.cursor.fetchall()
        self.cursor.execute("COMMIT;")
        self.connection.commit()
        return res

    def commit(self, push: bool = True):
        logger.debug("dolt add")
        self.cursor.execute("call DOLT_ADD('.');")
        logger.debug("dolt commit")
        try:
            self.cursor.execute("call DOLT_COMMIT('-m', 'partial import');")
        except pymysql.err.OperationalError as e:
            if "nothing to commit" not in str(e):
                raise
        if push:
            logger.debug("dolt push")
            self.cursor.execute("call DOLT_PUSH();")
        self.garbage_collect()

    @contextmanager
    def set_branch(self, branch: str):
        previous_branch = self.active_branch
        self.cursor.execute("call DOLT_CHECKOUT(%s)", branch)
        self.active_branch = branch
        yield
        self.cursor.execute("call DOLT_CHECKOUT(%s)", previous_branch)
        self.active_branch = previous_branch

    def pull_branch(self, branch: str, remote: str):
        with self.set_branch(branch):
            self.cursor.execute("call DOLT_PULL(%s, %s)", (remote, branch))