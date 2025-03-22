#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Code for interacting directly with SQL"""

import os
import random
from typing import Iterable, Tuple

SOURCES_SQL = """
    INSERT INTO sources (`annex-key`, sources)
    VALUES (%s, %s) as new(new_key, new_sources)
    ON DUPLICATE KEY UPDATE
    sources = JSON_MERGE_PATCH(sources, new_sources);
"""

ANNEX_KEYS_SQL = """
    INSERT INTO `annex-keys` (url, `annex-key`)
    VALUES (%s, %s) as new(new_url, new_key)
    ON DUPLICATE KEY UPDATE
    `annex-key` = new_key
"""

HASHES_SQL = """
    INSERT IGNORE INTO hashes (`hash`, `hashType`, `annex-key`) VALUES (%s, %s, %s);
"""

LOCAL_KEYS_SQL = """
    INSERT IGNORE INTO local_keys (`annex-key`) values (%s);
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
    next_choices = []
    if len(prefix) == len(min_key):
        next_choices.append("")
    lower = ord(min_key[len(prefix)])
    if lower < ord('a'):
        next_choices.append(chr(lower))
        lower = ord('a')
    upper = ord(max_key[len(prefix)])
    if upper > ord('z'):
        next_choices.append(chr(upper))
        upper = ord('z')
    next_choices.extend(map(chr, range(lower, upper+1)))

    print(next_choices)
    return prefix + random.choice(next_choices)


def random_batch(url_prefix: str, cursor, batch_size: int) -> Tuple[Iterable[str], int]:
    '''Get a random batch of urls with a given prefix'''

    cursor.execute("SELECT MIN(`url`), MAX(`url`) FROM `annex-keys` WHERE `annex-key` IS NULL and url LIKE %s;", (url_prefix+"%",))
    min_key, max_key = cursor.fetchone()
    pivot_key = random_string_key(min_key, max_key)
    # TODO: This query isn't using the full index.
    num_results = cursor.execute("SELECT url FROM `annex-keys` WHERE `annex-key` <=> NULL AND url >= %s LIMIT %s", (pivot_key, batch_size))
    return (row[0] for row in cursor.fetchall()), num_results
