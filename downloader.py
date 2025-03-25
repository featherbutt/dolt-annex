#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections.abc import Callable
import os
import subprocess
from typing import Dict, List

import pymysql

import annex
from annex import WEB_UUID, AnnexCache
import db
from dolt import DoltSqlServer
from dry_run import dry_run
from logger import logger
from git import Git
from move_functions import MoveFunction
from type_hints import UUID, AnnexKey, PathLike

class GitAnnexDownloader:

    git: Git
    local_uuid: UUID
    max_extension_length: int
    dolt_server: DoltSqlServer

    def __init__(self, cache: AnnexCache,
                 git: Git, dolt_server: DoltSqlServer):
        self.git = git
        self.cache = cache
        self.local_uuid = UUID(git.config['annex.uuid'])
        self.max_extension_length = int(git.config.get('annex.maxextensionlength', 4))
        logger.info(f"Local UUID: {self.local_uuid}")
        self.dolt_server = dolt_server
        # Create personal branch if it doesn't exist.
        # TODO: Make a branch with no parents.
        try:
            self.dolt_server.execute("CALL DOLT_BRANCH(%s);", (self.local_uuid,))
            with self.dolt_server.set_branch(self.local_uuid):
                self.dolt_server.execute(db.PERSONAL_BRANCH_INIT_SQL, [])
                self.dolt_server.commit(False)
        except pymysql.err.OperationalError as e:
            if "already exists" not in str(e):
                raise

    @dry_run("Would record that uuid {uuid} is a source for this remote")
    def add_local_source(self, key: AnnexKey):
        """Add a source to the database for a key"""
        # self.cache.mark_present(key)
        self.cache.insert_source(key, self.local_uuid)

    def add_remote_source(self, key: AnnexKey, uuid: UUID):
        """Add a source to the database for a key"""
        self.cache.insert_source(key, uuid)

    @dry_run("Would record that uuid {uuid} is a source for key {key}")
    def add_source(self, key: AnnexKey, uuid: UUID):
        """Add a source to the database for a key"""
        self.cache.insert_source(key, uuid)

    @dry_run("Would record the we have a copy of key {key} from url {url}")
    def update_database(self, url: str, key: AnnexKey):
        self.cache.insert_url(key, url)
        self.cache.insert_source(key, WEB_UUID)

    @dry_run("Would record that key {key} has md5 {md5}")
    def record_md5(self, md5: str, key: str):
        md5bytes = bytes.fromhex(md5)
        self.cache.insert_md5(key, md5bytes)

    def import_git_branch(self, other_repo: str, branch: str, url_from_path: Callable[[str], List[str]], follow_symlinks: bool = False):
        """Import a git branch into the annex. Currently unused."""
        # Stream the git ls-tree output
        git = Git(other_repo)

        process = git.popen('ls-tree', '-r', branch, stdout=subprocess.PIPE, text=True)

        # Process files as they come in
        for line in process.stdout:
            objectmode, objecttype, objecthash, filename = line.strip().split()
            # TODO: Handle symlinks.
            contents = git.show(branch, filename)
            if contents:
                symlink = contents.strip()
                key = symlink.split('/')[-1]
                urls = url_from_path(symlink)
                for url in urls:
                    self.cache.insert_url(key, url)
        # Make sure the process completed successfully
        retcode = process.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(retcode, 'git ls-tree')

    def mark_present_keys(self):
        """Record the keys that are present in the annex"""
        # TODO: Account for non-bare repos
        for _, _, files in os.walk(os.path.join(self.git.git_dir, 'annex', 'objects')):
            for file in files:
                # The key is the filename
                key = file
                logger.debug(f"marking {key} as present")
                self.cache.mark_present(key)

    def discover_and_populate(self, record_urls: bool, record_sources: bool):
        """Walk the git-annex branch and populate the database"""

        # Stream the git ls-tree output
        process = subprocess.Popen(
            ['git', '-C', self.git.git_dir, 'ls-tree', '-r', 'git-annex', '--name-only'],
            stdout=subprocess.PIPE,
            text=True
        )

        # Process files as they come in
        assert process.stdout is not None
        for line in process.stdout:
            filename = line.strip()
            if '/' not in filename:
                continue
            if filename.endswith('.log'):
                key = AnnexKey(os.path.splitext(os.path.basename(filename))[0])
                logger.log(f"Processing key: {key}")

                if record_sources:
                    log_content = self.git.show('git-annex', filename)
                    if log_content:
                        uuid_dict = annex.parse_log_file(log_content)
                        for source in uuid_dict or []:
                            self.cache.insert_source(key, source)

                if record_urls:
                    # Check for web URLs
                    web_log = f"{os.path.splitext(filename)[0]}.log.web"
                    web_content = self.git.show('git-annex', web_log)
                    if web_content:
                        urls = annex.parse_web_log(web_content)
                        for url in urls:
                            self.cache.insert_url(key, url)

        # Make sure the process completed successfully
        retcode = process.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(retcode, 'git ls-tree')

    def flush(self):
        self.cache.flush()

def move_files(downloader: GitAnnexDownloader, move: MoveFunction, files: Dict[AnnexKey, PathLike]):
    """Move files to the annex"""
    logger.debug("moving annex files")
    for key, file_path in files.items():
        key_path = downloader.git.annex.get_annex_key_path(key)
        move(file_path, key_path)
    files.clear()
