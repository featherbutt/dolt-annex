#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from uuid import UUID

from typing_extensions import Dict


from annex import AnnexCache
from config import config
import db
from dolt import DoltSqlServer
from dry_run import dry_run
from git import get_key_path
from logger import logger
from move_functions import MoveFunction
from type_hints import AnnexKey, PathLike

class GitAnnexDownloader:

    max_extension_length: int
    dolt_server: DoltSqlServer
    cache: AnnexCache

    def __init__(self, cache: AnnexCache, dolt_server: DoltSqlServer):
        self.cache = cache
        self.max_extension_length = 4
        self.files_dir = config.get().files_dir
        local_uuid = config.get().local_uuid
        logger.info(f"Local UUID: {local_uuid}")
        self.dolt_server = dolt_server
        # Initialize the local branch if it doesn't exist
        with self.dolt_server.maybe_create_branch(local_uuid.hex):
            for query in db.PERSONAL_BRANCH_INIT_SQL:
                self.dolt_server.execute(query, [])
            self.dolt_server.commit(False)

    @dry_run("Would record that uuid {uuid} is a source for this remote")
    def add_local_source(self, key: AnnexKey):
        """Add a source to the database for a key"""
        # self.cache.mark_present(key)
        # self.cache.insert_key_source(key, self.local_uuid)

    def add_remote_source(self, key: AnnexKey, uuid: UUID):
        """Add a source to the database for a key"""
        # self.cache.insert_key_source(key, uuid)

    @dry_run("Would record that uuid {uuid} is a source for key {key}")
    def add_source(self, key: AnnexKey, uuid: UUID):
        """Add a source to the database for a key"""
        # self.cache.insert_key_source(key, uuid)

    @dry_run("Would record the we have a copy of key {key} from url {url}")
    def update_database(self, url: str, key: AnnexKey):
        """Record that we have a copy of a key from a URL"""
        # self.cache.insert_url(key, url)
        # self.cache.insert_key_source(key, WEB_UUID)

    @dry_run("Would record that key {key} has md5 {md5}")
    def record_md5(self, md5: str, key: str):
        md5bytes = bytes.fromhex(md5)
        self.cache.insert_md5(key, md5bytes)

    def mark_present_keys(self):
        """Record the keys that are present in the annex"""
        # TODO: Account for non-bare repos
        for _, _, files in os.walk('.'):
            for file in files:
                # The key is the filename
                key = file
                logger.debug(f"marking {key} as present")
                self.cache.mark_present(key)

    def flush(self):
        self.cache.flush()

def move_files(move: MoveFunction, files: Dict[AnnexKey, PathLike]):
    """Move files to the annex"""
    logger.debug("moving annex files")
    base_config = config.get()
    files_dir = os.path.abspath(base_config.files_dir)
    for key, file_path in files.items():
        key_path = PathLike(os.path.join(files_dir, get_key_path(key)))
        move(file_path, key_path)
    files.clear()
