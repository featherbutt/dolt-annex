from contextlib import contextmanager
import time
from typing import Dict

from plumbum import local
import pymysql

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
                logger.log(f"Waiting for SQL server: {str(e)}")
                time.sleep(1)

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

    def executemany(self, sql: str, values):
        self.cursor.executemany(sql, values)
        self.cursor.execute("COMMIT;")
        self.connection.commit()

    def execute(self, sql: str, values):
        self.cursor.execute(sql, values)
        self.cursor.execute("COMMIT;")
        self.connection.commit()