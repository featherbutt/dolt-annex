from dataclasses import dataclass
import os
import random
from contextlib import contextmanager
from typing import Tuple

from logger import logger

import pymysql

class BatchInserter:
    def __init__(self, dolt_server, sql):
        self.dolt_server = dolt_server
        self.sql = sql
        self.values = []
        
    def insert(self, *row):
        self.values.append(row)
        
    def flush(self):
        self.dolt_server.executemany(self.sql, self.values)
        self.values.clear()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

sources_sql = """
    INSERT INTO sources (`annex-key`, sources)
    VALUES (%s, %s) as new(new_key, new_sources)
    ON DUPLICATE KEY UPDATE
    sources = JSON_MERGE_PATCH(sources, new_sources);
"""

annex_keys_sql = """
    INSERT INTO `annex-keys` (url, `annex-key`)
    VALUES (%s, %s) as new(new_url, new_key)
    ON DUPLICATE KEY UPDATE
    `annex-key` = new_key
"""

hashes_sql = """
    INSERT IGNORE INTO hashes (`hash`, `hashType`, `annex-key`) VALUES (%s, %s, %s);
"""

# What follows is a hack. Ideally we'd like to get a page of urls by simply executing a query like:
#
# SELECT url FROM `annex-keys` WHERE `annex-key` <=> NULL OFFSET X LIMIT 1000;
# 
# Unfortunately, Dolt doesn't currently optimize OFFSET. So instead, we need to generate a random value
# to use as a lower bound for the `url` column.

def random_string_key(min_key, max_key: str) -> str:
    '''Generate a random string key between min_key and max_key'''
    prefix = os.path.commonprefix([min_key, max_key])
    nextChoices = []
    if len(prefix) == len(min_key):
        nextChoices.append("")
    lower = ord(min_key[len(prefix)])
    if lower < ord('a'):
        nextChoices.append(chr(lower))
        lower = ord('a')
    upper = ord(max_key[len(prefix)])
    if upper > ord('z'):
        nextChoices.append(chr(upper))
        upper = ord('z')
    nextChoices.extend(map(chr, range(lower, upper+1)))

    print(nextChoices)
    return prefix + random.choice(nextChoices)


def random_batch(url_prefix: str, cursor, batch_size: int) -> Tuple[list[str], int]:
    '''Get a random batch of urls with a given prefix'''

    cursor.execute("SELECT MIN(`url`), MAX(`url`) FROM `annex-keys` WHERE `annex-key` IS NULL and url LIKE %s;", (url_prefix+"%",))
    min_key, max_key = cursor.fetchone()
    pivot_key = random_string_key(min_key, max_key)
    # TODO: This query isn't using the full index.
    num_results = cursor.execute("SELECT url FROM `annex-keys` WHERE `annex-key` <=> NULL AND url >= %s LIMIT %s", (pivot_key, batch_size))
    return (row[0] for row in cursor.fetchall()), num_results