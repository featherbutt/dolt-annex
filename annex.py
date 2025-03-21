#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This file contains functions for interacting with git-annex"""

from dataclasses import dataclass
import json
import os
import pathlib
import time
from typing import Callable, Dict, List, Set

from bup.repo.base import RepoProtocol as Repo

from bup_ext.patch import DirectoryPatch, update_file
from bup_ext.bup_ext import CommitMetadata, apply_patch
import sql
from dolt import DoltSqlServer
from git import Git
from logger import logger

# reserved git-annex UUID for the web special remote
WEB_UUID = '00000000-0000-0000-0000-000000000001'

def parse_log_file(content: str) -> Set[str]:
    """Parse a .log file and return a set of UUIDs that have the file"""
    uuids = set()
    for line in content.splitlines():
        if not line.strip():
            continue
        try:
            timestamp, present, uuid = line.split()
            if present == "1" and uuid != WEB_UUID:
                uuids.add(uuid)
        except ValueError:
            print(f"Warning: malformed log line: {line}")
    return uuids

def parse_web_log(content: str) -> List[str]:
    """Parse a .log.web file and return a list of URLs"""
    urls = []
    for line in content.splitlines():
        if not line.strip():
            continue
        try:
            timestamp, present, url = line.split(maxsplit=2)
            if present == "1":
                urls.append(url)
        except ValueError:
            print(f"Warning: malformed web log line: {line}")
    return urls

# We must prevent data loss in the event the process is interrupted:
# - Original file names contain data that is lost when the file is added to the annex
# - Adding a file to the annex without updating the database can result in the file being effectively lost
# - The context manager ensures that the database cache will be flushed if the process is terminated, but this is not sufficient
#   in the event of SIGKILL, power loss, or other catastrophic failure, or if the flush fails.
# - But if we commit the database entries before adding the annex files, if the files don't get moved and we might not re-add them.
# - But we can just check what files remain in the import directoy.
# - So we have a separate branch
# The safe approach is the following:
# - Add the database entries
# - After flushing the database cache, compute the new git-annex branch.
# - Move the annex files in a batch.

@dataclass
class GitAnnexSettings:
    commit_metadata: CommitMetadata
    ref: bytes

MoveFunction = Callable[[str, str], None]
    
class AnnexCache:
    """The AnnexCache allows for batched operations against the git-annex branch and the Dolt database."""
    urls: Dict[str, List[str]]
    md5s: Dict[str, bytes]
    sources: Dict[str, List[str]]
    local_keys: Set[str]
    files: Dict[str, str]
    git: Git
    dolt: DoltSqlServer
    move: MoveFunction
    auto_push: bool
    batch_size: int
    count: int
    time: float

    def __init__(self, repo: Repo, dolt: DoltSqlServer, git: Git, git_annex_settings: GitAnnexSettings, move: MoveFunction, auto_push: bool, batch_size: int):
        self.repo = repo
        self.dolt = dolt
        self.git = git
        self.urls = {}
        self.md5s = {}
        self.sources = {}
        self.files = {}
        self.local_keys = set()
        self.git_annex_settings = git_annex_settings
        self.batch_size = batch_size
        self.count = 0
        self.move = move
        self.time = time.time()
        self.local_uuid = git.config['annex.uuid']
        self.auto_push = auto_push

    def increment_count(self):
        self.count += 1
        if self.count >= self.batch_size:
            self.flush()
            self.count = 0

    def insert_url(self, key: str, url: str):
        if key not in self.urls:
            self.urls[key] = []
        self.urls[key].append(url)
        self.increment_count()
        

    def insert_md5(self, key: str, md5: bytes):
        self.md5s[key] = md5
        self.increment_count()

    def insert_source(self, key: str, source: str):
        if key not in self.sources:
            self.sources[key] = []
        self.sources[key].append(source)
        self.increment_count()

    def insert_file(self, key: str, filename: str):
        self.files[key] = filename
        self.increment_count()

    def mark_present(self, key: str):
        self.local_keys.add(key)
        self.increment_count()

    def flush(self):
        # Flushing the cache must be done in the following order:
        # 1. Update the git-annex branch to contain the new ownership records and registered urls.
        # 2. Update the Dolt database to match the git-annex branch.
        # 3. Move the annex files to the annex directory. This step is a no-op when running the downloader,
        #    because downloaded files were alreafy written into the annex.
        # This way, if the import process is interrupted, all incomplete files will still exist in the source directory.
        # Likewise, if a download process is interrupted, the database will still indicate which files have been downloaded.

        logger.debug("flushing cache")
        if not self.urls and not self.md5s and not self.sources and not self.files and not self.local_keys:
            return
        
        now = bytes(str(int(time.time())), encoding="utf8") + b"s"
        patch = DirectoryPatch()
        def insert(key_values: Dict[str, List[str]], suffix: bytes):
            for key, values in key_values.items():
                key_path = bytes(self.git.annex.get_branch_key_path(key), encoding="utf8")
                file_path = key_path + suffix
                rows = [[now, b"1", bytes(value, encoding = "utf8")] for value in values]
                patch.insert(file_path, update_file(rows, 2))
        
        if self.sources or self.urls:
            logger.debug("creating git-annex patch")
            insert(self.sources, b".log")
            insert(self.urls, b".log.web")
            logger.debug("applying git-annex patch")
            apply_patch(self.repo, self.git_annex_settings.ref, self.git_annex_settings.ref, patch, self.git_annex_settings.commit_metadata)

        # 2. Update the Dolt database to match the git-annex branch.

        logger.debug("flushing dolt database")
        if self.sources:
            self.dolt.executemany(sql.SOURCES_SQL, [(key, json.dumps({source: 1 for source in sources})) for key, sources in self.sources.items()])
        if self.urls:
            self.dolt.executemany(sql.ANNEX_KEYS_SQL, [(url, key) for key, urls in self.urls.items() for url in urls])
        if self.md5s:
            self.dolt.executemany(sql.HASHES_SQL, [(md5, 'md5', key) for key, md5 in self.md5s.items()])
        if self.local_keys:
            with self.dolt.set_branch(self.local_uuid):
                self.dolt.executemany(sql.LOCAL_KEYS_SQL, [(key,) for key in self.local_keys])
                self.dolt.commit(self.auto_push)

        # 3. Move the annex files to the annex directory.

        logger.debug("moving annex files")
        for key, file_path in self.files.items():
            key_path = self.git.annex.get_annex_key_path(key)
            pathlib.Path(os.path.dirname(key_path)).mkdir(parents=True, exist_ok=True)
            self.move(file_path, key_path)
        
        num_keys = max(len(self.urls), len(self.md5s), len(self.sources), len(self.files), len(self.local_keys))
        self.urls.clear()
        self.md5s.clear()
        self.sources.clear()
        self.files.clear()
        self.local_keys.clear()

        logger.debug("pushing dolt database")
        self.dolt.commit(self.auto_push)

        new_now = time.time()
        elapsed_time = new_now - self.time
        logger.debug(f"added {num_keys} keys in {elapsed_time:.2f} seconds")
        self.time = new_now

        # TODO: Only auto-push content to server if we can do it efficiently.
        # ie. git-annex won't make a commit per-file.
        # if self.auto_push:
        # self.git.annex.push_content(key)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()