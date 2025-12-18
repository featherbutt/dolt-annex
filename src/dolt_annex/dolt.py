#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Functionality for interacting with the Dolt server."""

import os
from pathlib import Path
import time
from uuid import UUID

from typing_extensions import Any, Dict, Tuple

from plumbum import local # type: ignore
import pymysql

from dolt_annex.datatypes.table import DatasetSchema
from dolt_annex.logger import logger
from dolt_annex.datatypes.repo import Repo

class DoltSqlServer:
    """A connection to a Dolt SQL server."""
    db_config: Dict[str, Any]
    connection: pymysql.connections.Connection
    cursor: pymysql.cursors.Cursor
    active_branch: str
    db_name: str

    def __init__(self, dolt_dir: Path, dolt_db_name: str, db_config: Dict[str, Any], spawn_dolt_server: bool):
        self.db_config = db_config
        self.db_name = dolt_db_name

        if spawn_dolt_server:
            self.dolt_server_process, self.connection = self.spawn_dolt_server(dolt_dir)
        else:
            self.dolt_server_process = None
            self.connection = pymysql.connect(**db_config)

        self.cursor = self.connection.cursor()

        self.cursor.execute("SELECT ACTIVE_BRANCH()")
        res = self.cursor.fetchone()
        assert res is not None
        self.active_branch = res[0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.dolt_server_process:
            self.dolt_server_process.terminate()

    def spawn_dolt_server(self, dolt_dir: Path) -> Tuple[Any, pymysql.connections.Connection]:
        dolt = local.cmd.dolt.with_cwd(dolt_dir)
        args = []
        if "port" in self.db_config:
            args.extend(["-P", str(self.db_config["port"])])
        if "unix_socket" in self.db_config:
            args.extend(["--socket", self.db_config["unix_socket"]])
        if (server_logfile := os.getenv("DA_SERVER_LOGFILE")) is not None:
            dolt = (dolt > server_logfile)
        dolt_server_process = dolt.popen(["sql-server", *args])
        while True:
            try:
                return dolt_server_process, pymysql.connect(**self.db_config)
            except Exception as e:
                logger.verbose(f"Waiting for SQL server: {str(e)}")
                time.sleep(1)

    def executemany(self, sql: str, values):
        self.cursor.executemany(sql, values)
        self.cursor.execute("COMMIT;")
        self.connection.commit()
    
    def execute(self, sql: str, values):
        self.cursor.execute(sql, values)
        self.cursor.fetchall()
        self.cursor.execute("COMMIT;")
        self.connection.commit()
    
    def query(self, sql: str, values = ()):
        cursor = self.connection.cursor()
        cursor.execute(sql, values)
        res = cursor.fetchmany()
        while res:
            yield from res
            res = cursor.fetchmany()
        cursor.execute("COMMIT;")
        self.connection.commit()

    def commit(self, amend: bool = False):
        logger.debug("dolt add")
        self.cursor.execute("call DOLT_ADD('.');")
        logger.debug("dolt commit")
        try:
            if amend:
                self.cursor.execute("call DOLT_COMMIT('--amend');")
            else:
                self.cursor.execute("call DOLT_COMMIT('-m', 'partial import');")
        except pymysql.err.OperationalError as e:
            if "nothing to commit" not in str(e):
                raise

    def maybe_create_branch(self, branch: str, start_point: str = "HEAD"):
        """
        Return the named branch, creating it from start_point if it doesn't exist.

        The returned branch can be used as a context manager to switch back to the original branch
        when done. This is useful for creating a branch and then switching to it.
        """
        # Kind of a hack, but we may need to pull the start point branch from upstream if it doesn't exist locally.
        # But the start point may also not be a branch, so...
        try:
            with DoltBranch(self, start_point):
                pass
        except pymysql.err.OperationalError as e:
            raise DoltException(f"Failed to find start point {start_point}") from e

        try:
            self.cursor.execute("call DOLT_BRANCH(%s, %s);", (branch, start_point))
        except pymysql.err.OperationalError as e:
            if "already exists" not in str(e):
                raise DoltException(f"Failed to create branch {branch} from {start_point}") from e
        return DoltBranch(self, branch)
        
    def set_branch(self, branch: str):
        """
        Set the active branch to the given branch.
        
        This can be used as a context manager to automatically switch
        back to the previous branch when done."""
        return DoltBranch(self, branch)

    def pull_branch(self, branch: str, remote: Repo):
        with self.set_branch(branch):
            self.cursor.execute("call DOLT_PULL(%s, %s)", (remote.name, branch))

    def push_branch(self, branch: str, remote: Repo):
        with self.set_branch(branch):
            self.cursor.execute("call DOLT_PUSH(%s, %s)", (remote.name, branch))
            res = self.cursor.fetchone()
            assert res is not None
            status, _ = res
            if status != 0:
                # In the event of a conflict, attempt merging first.
                logger.debug(f"Potential conflict, attempting to merge {branch} with {remote}")
                self.pull_branch(branch, remote)
                self.cursor.execute("call DOLT_PUSH(%s, %s)", (remote, branch))
                res = self.cursor.fetchone()
                assert res is not None
                status, _ = res
                if status != 0:
                    raise DoltException(f"Failed to push {branch} to {remote} after merge")

    def get_revision(self, ref: str):
        self.cursor.execute("SELECT DOLT_HASHOF(%s);", ref)
        res = self.cursor.fetchone()
        assert res is not None
        return res[0]

    def merge(self, branch: str):
        """Merge the given branch into the current branch."""
        with self.set_branch(branch):
            self.commit(amend=True)
        try:
            self.cursor.execute("call DOLT_MERGE(%s);", (branch,))
        except pymysql.err.OperationalError as e:
            if "nothing to merge" not in str(e):
                raise DoltException(f"Failed to merge {branch} into {self.active_branch}") from e
        res = self.cursor.fetchone()
        assert res is not None
        _, _, conflicts, _ = res
        if conflicts > 0:
            self.cursor.execute("call DOLT_MERGE('--abort');")
            raise DoltException(f"Failed to merge {branch} into {self.active_branch}: unresolvable conflicts detected")
        
    def initialize_dataset_source(self, dataset_schema: DatasetSchema, repo_uuid: UUID):
        """
        Ensures that the Dolt repo contains the necessary branches for this dataset.
        """
        self.maybe_create_branch(f"{repo_uuid}-{dataset_schema.name}", dataset_schema.empty_table_ref)

class DoltException(Exception):
    """Exception raised for errors when executing Dolt commands."""

class DoltBranch:
    """Represents a branch in a Dolt repository.
    
    This can be used as a context manager to switch to the branch
    and automatically switch back to the previous branch when done.
    """

    previous_branches: list[str]

    def __init__(self, dolt: DoltSqlServer, branch: str):
        self.dolt = dolt
        self.branch = branch
        self.previous_branches = []

    def __enter__(self):
        self.previous_branches.append(self.dolt.active_branch)
        self.dolt.cursor.execute("call DOLT_CHECKOUT(%s, '--')", self.branch)
        self.dolt.active_branch = self.branch
        return self.dolt.set_branch(self.branch)

    def __exit__(self, exc_type, exc_value, traceback):
        active_branch = self.previous_branches.pop()
        self.dolt.cursor.execute("call DOLT_CHECKOUT(%s, '--')", active_branch)
        self.dolt.active_branch = active_branch
