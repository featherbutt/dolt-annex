from collections.abc import Callable
import os
import tempfile
import subprocess
import requests
import json
from typing import List, Optional

import annex
import db
from logger import logger
from git import Git

class GitAnnexDownloader:
        
    def __init__(self, 
                 git: Git, sources: db.BatchInserter, annex_keys: db.BatchInserter, cursor, auto_push: bool, batch_size: int = 10):
        self.git = git
        self.local_uuid = git.config['annex.uuid']
        self.max_extension_length = int(git.config.get('annex.maxextensionlength', 4))
        logger.log(f"Local UUID: {self.local_uuid}")
        self.sources = sources
        self.cursor = cursor
        self.annex_keys = annex_keys
        self.batch_size = batch_size
        self.auto_push = auto_push

    def add_source(self, key: str, uuid: str):
        """Add a source to the database for a key"""
        self.sources.insert(key, json.dumps({uuid: 1}))

    def update_database(self, url: str, key: str):
        """Update the database with the new key for a URL and initialize sources"""
        self.add_source(key, self.local_uuid)
        self.annex_keys.insert(url, key)

    def add_file(self, filename: str) -> str:
        key = self.git.annex.calckey(filename)
        self.git.annex.setkey(key, filename)
        if self.auto_push:
            self.git.annex.push_content(key)
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
                self.git.annex.registerurl(key, url)

                self.update_database(url, key)
                
                return key
                
            except (requests.RequestException, subprocess.CalledProcessError, ValueError) as e:
                print(f"Error processing URL {url}: {str(e)}")
                return None
            
    def import_file(self, path: str, url_from_path: Callable[[str], List[str]] = None) -> str:
        """Import a file into the annex"""
        extension = os.path.splitext(path)[1]
        if len(extension) > self.max_extension_length+1:
            return
        key = self.add_file(path)
        self.add_source(key, self.local_uuid)

        if url_from_path:
            urls = url_from_path(path)
            for url in urls:
                self.annex_keys.insert(url, key)

        return key

    def import_directory(self, path: str, url_from_path: Callable[[str], List[str]] = None):
        """Import a directory into the annex"""
        for root, _, files in os.walk(path):
            for file in files:
                self.import_file(os.path.join(root, file), url_from_path)
            

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

