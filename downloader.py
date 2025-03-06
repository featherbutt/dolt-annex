from collections.abc import Callable
import os
from sqlite3 import Cursor
import tempfile
import subprocess
import requests
import json
from typing import List, Optional, Tuple

import annex
import db
from dolt import DoltSqlServer
from dry_run import dry_run
import importers
from logger import logger
from git import Git


class GitAnnexDownloader:
        
    git: Git
    local_uuid: str
    max_extension_length: int
    dolt_server: DoltSqlServer
    sources: db.BatchInserter
    annex_keys: db.BatchInserter
    hashes: db.BatchInserter
    batch_size: int
    auto_push: bool
    annex_queue: List[Tuple[str,str]]

    def __init__(self, 
                 git: Git, sources: db.BatchInserter, annex_keys: db.BatchInserter, hashes: db.BatchInserter, dolt_server: DoltSqlServer, auto_push: bool, batch_size):
        self.git = git
        self.local_uuid = git.config['annex.uuid']
        self.max_extension_length = int(git.config.get('annex.maxextensionlength', 4))
        logger.info(f"Local UUID: {self.local_uuid}")
        self.sources = sources
        self.dolt_server = dolt_server
        self.annex_keys = annex_keys
        self.hashes = hashes
        self.batch_size = batch_size
        self.auto_push = auto_push
        self.annex_queue = []

    @dry_run("Would record that uuid {uuid} is a source for key {key}")
    def add_source(self, key: str, uuid: str):
        """Add a source to the database for a key"""
        self.sources.insert(key, json.dumps({uuid: 1}))

    @dry_run("Would record the we have a copy of key {key} from url {url}")
    def update_database(self, url: str, key: str):
        self.git.annex.registerurl(key, url)
        self.annex_keys.insert(url, key)

    def add_file(self, filename: str) -> str:
        key = self.git.annex.calckey(filename)
        self.annex_queue.append((key, filename))
        return key
                

    def download_file(self, url: str) -> Optional[str]:
        """Download a file from a url, add it to the annex, and update the database"""
        extension = os.path.splitext(url)[1]
        suffix = None
        if len(extension) <= self.max_extension_length+1:
            suffix = extension
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                temp_file.flush()
                
                key = self.add_file(temp_file.name)
                
                self.add_source(key, self.local_uuid)
                self.update_database(url, key)

                if len(self.annex_queue) >= self.batch_size:
                    self.flush()
                
                return key
                
            except (requests.RequestException, subprocess.CalledProcessError, ValueError) as e:
                print(f"Error processing URL {url}: {str(e)}")
                return None
            
    def import_file(self, path: str, importer: importers.Importer) -> str:
        """Import a file into the annex"""
        extension = os.path.splitext(path)[1]
        if len(extension) > self.max_extension_length+1:
            return
        if extension == 'lnk':
            return
        key = self.add_file(path)
        self.add_source(key, self.local_uuid)

        if importer:
            urls = importer.url(path)
            for url in urls:
                self.update_database(url, key)
            if (md5 := importer.md5(path)):
                self.record_md5(md5, key)
        
        if len(self.annex_queue) >= self.batch_size:
            self.flush()

        return key
    
    @dry_run("Would record that key {key} has md5 {md5}")
    def record_md5(self, md5: str, key: str):
        md5bytes = bytes.fromhex(md5)
        self.hashes.insert(md5bytes, "md5", key)

    def import_directory(self, path: str, importer: importers.Importer):
        """Import a directory into the annex"""
        for root, _, files in os.walk(path):
            for file in files:
                self.import_file(os.path.join(root, file), importer)

    def import_git_branch(self, other_repo: str, branch: str, url_from_path: Callable[[str], List[str]] = None):
        """Import a git branch into the annex"""
        # Stream the git ls-tree output
        git = Git(other_repo)
        
        process = git.popen('ls-tree', '-r', branch, stdout=subprocess.PIPE, text=True)
        
        # Process files as they come in
        for line in process.stdout:
            objectmode, objecttype, objecthash, filename = line.strip().split()
            if objectmode != '120000':
                continue
            contents = git.show(branch, filename)
            if contents:
                symlink = contents.strip()
                key = symlink.split('/')[-1]
                urls = url_from_path(symlink)
                for url in urls:
                    self.annex_keys.insert(url, key)
        # Make sure the process completed successfully
        retcode = process.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(retcode, 'git ls-tree')
        
            

    def discover_and_populate(self):
        """Walk the git-annex branch and populate the database"""
        
        # Stream the git ls-tree output
        process = subprocess.Popen(
            ['git', '-C', self.git.git_dir, 'ls-tree', '-r', 'git-annex', '--name-only'],
            stdout=subprocess.PIPE,
            text=True
        )
        
        # Process files as they come in
        for line in process.stdout:
            filename = line.strip()
            if '/' not in filename:
                continue
            if filename.endswith('.log'):
                key = os.path.splitext(os.path.basename(filename))[0]
                logger.log(f"Processing key: {key}")
                
                log_content = self.git.show('git-annex', filename)
                if log_content:
                    uuid_dict = annex.parse_log_file(log_content)
                    if uuid_dict:
                        self.sources.insert(key, json.dumps({key: 1 for key in uuid_dict}))
                
                # Check for web URLs
                web_log = f"{os.path.splitext(filename)[0]}.log.web"
                web_content = self.git.show('git-annex', web_log)
                if web_content:
                    urls = annex.parse_web_log(web_content)
                    for url in urls:
                        self.annex_keys.insert(url, key)

        # Make sure the process completed successfully
        retcode = process.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(retcode, 'git ls-tree')

    def flush(self):
        """Flush the database caches, then update the annex"""
        self.sources.flush()
        self.annex_keys.flush()
        self.hashes.flush()
        for key, filename in self.annex_queue:
            # TODO: Neither of these commands are batchable, but we could write our own packfile.
            self.git.annex.setkey(key, filename)
            if self.auto_push:
                self.git.annex.push_content(key)
        self.annex_queue = []