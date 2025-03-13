from contextlib import contextmanager
import time
from typing import Dict

from plumbum import local
import pymysql

from dry_run import dry_run
from logger import logger

class DoltSqlServer:

    db_config: Dict[str, str]
    connection: pymysql.connections.Connection
    cursor: pymysql.cursors.Cursor

    def __init__(self, dolt_dir: str, db_config: Dict[str, str], spawn_dolt_server: bool):
        self.db_config = db_config

        if spawn_dolt_server:
            self.dolt_server_process, self.connection = self.spawn_dolt_server(dolt_dir)
        else:
            self.dolt_server_process = None
            self.connection = pymysql.connect(**db_config)

        self.cursor = self.connection.cursor()
        self.garbage_collect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.dolt_server_process:
            self.dolt_server_process.terminate()

    def spawn_dolt_server(self, dolt_dir: str) -> pymysql.connections.Connection:
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
            if "no changes since last gc" in str(e):
                pass
            else:
                raise
        self.connection = pymysql.connect(**self.db_config)
        self.cursor = self.connection.cursor()

    @dry_run("Would execute {sql} with values {values}")
    def executemany(self, sql: str, values):
        logger.debug(f"flushing {len(values)} rows")
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

    def push(self):
        logger.debug(f"dolt add")
        self.cursor.execute("call DOLT_ADD('.');")
        logger.debug(f"dolt commit")
        try:
            self.cursor.execute("call DOLT_COMMIT('-m', 'partial import');")
        except pymysql.err.OperationalError as e:
            if "nothing to commit" in str(e):
                pass
        logger.debug(f"dolt push")
        self.cursor.execute("call DOLT_PUSH();")
        self.garbage_collect()