from contextlib import contextmanager
import time

from plumbum import local
import pymysql

@contextmanager
def DoltSqlServer(dolt_dir, db_config, spawn_dolt_server):
    if spawn_dolt_server:
        def wait_for_sql_server():
            while True:
                try:
                    return pymysql.connect(**db_config)
                except Exception as e:
                    print(f"Waiting for SQL server: {str(e)}")
                    time.sleep(1)
        dolt = local.cmd.dolt.with_cwd(dolt_dir)
        try:
            sql_server = dolt.popen("sql-server")
            yield wait_for_sql_server()
        finally:
            sql_server.terminate()
    else:
        yield pymysql.connect(**db_config)

