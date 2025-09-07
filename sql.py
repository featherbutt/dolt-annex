#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Code for interacting directly with SQL"""

import os
import random

from typing_extensions import Iterable, Tuple

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

LOCAL_SUBMISSIONS_SQL = """
    INSERT IGNORE INTO `local_submissions` (`source`, `id`, `updated`, `part`)
    VALUES (%s, %s, %s, %s);
"""

SUBMISSION_KEYS_SQL = """
    INSERT IGNORE INTO `file_keys` (`source`, `id`, `updated`, `part`, `file_key`)
    VALUES (%s, %s, %s, %s, %s);
"""

DIFF_SQL = """
SELECT
    `file_key`, `diff_type`, `to_source`, `to_id`, `to_updated`, `to_part`
FROM dolt_commit_diff_local_submissions
JOIN file_keys AS OF files
    ON source = to_source AND id = to_id AND updated = to_updated AND part = to_part
WHERE from_commit = HASHOF(%s) AND to_commit = HASHOF(%s) AND to_source = %s
"""