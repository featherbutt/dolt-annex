import sys
import os
import tempfile
import subprocess
import hashlib
import requests
import pymysql
import json
from typing import Optional, Tuple, Dict, Set, List
from contextlib import contextmanager

class Git:
    def __init__(self, path: str):
        self.path = path

    def run(self, *args, **kwargs):
        result = subprocess.run(
            ['git', '-C', self.path, *args],
            capture_output=True,
            text=True,
            check=True,
            **kwargs
        )
        return result.stdout.strip()

    class Config:
        def __init__(self, git: 'Git'):
            self.git = git
            
        def __getitem__(self, key: str) -> str:
            return self.git.run('config', key)
    
        def __setitem__(self, key: str, value: str):
            self.git.run('config', key, value)
        
        def __delitem__(self, key: str):
            self.git.run('config', '--unset', key)

    @property
    def config(self):
        return self.Config(self)

class Logger:
    def __init__(self, log: callable):
        self.log = log

    @contextmanager
    def section(self, name):
        self.log(f"Starting {name}...")
        yield
        self.log(f"Finished {name}")

    def method(self, method):
        def wrapper(*args, **kwargs):
            with self.section(method.__name__):
                return method(*args, **kwargs)
        return wrapper
    
null_logger = Logger(lambda *args, **kwargs: None)
logger = Logger(print)

class GitAnnexDownloader:
    def __init__(self, 
                 git: Git, local_uuid: str, batch_size: int = 10,
                 db_batch_size: int = 1000):
        self.db_config = {
            "unix_socket": "/tmp/mysql.sock",
            "user": "root",
            "database": "dolt"
        }
        self.git = git
        self.local_uuid = local_uuid
        self.batch_size = batch_size
        self.db_batch_size = db_batch_size

    @contextmanager
    def db_connection(self):
        """Context manager for database connections"""
        connection = pymysql.connect(**self.db_config)
        try:
            yield connection
        finally:
            connection.close()

    def fetch_unprocessed_urls(self) -> list[str]:
        """Fetch a batch of URLs that haven't been processed yet"""
        with self.db_connection() as connection:
            with connection.cursor() as cursor:
                sql = """
                    SELECT url 
                    FROM `annex-keys` 
                    WHERE `annex-key` IS NULL 
                    LIMIT %s
                """
                cursor.execute(sql, (self.batch_size,))
                return (row[0] for row in cursor.fetchall())

    def compute_annex_key(self, file_path: str) -> str:
        """Compute the git-annex key for a file without adding it to the repository"""
        return self.git.run('annex', 'calckey', file_path)

    def register_url(self, key: str, url: str):
        """Register a URL for a key in git-annex"""
        return self.git.run('annex', 'registerurl', key, url)

    def reinject_file(self, key: str, file_path: str) -> str:
        """Reinject a file into git-annex and return its key"""
        print(key, file_path)
        return self.git.run('annex', 'setkey', key, file_path)

    def update_database(self, url: str, key: str):
        """Update the database with the new key for a URL and initialize sources"""
        with self.db_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    # Begin transaction
                    connection.begin()
                    
                    # Update annex-keys table
                    sql = """
                        UPDATE `annex-keys` 
                        SET `annex-key` = %s 
                        WHERE url = %s
                    """
                    cursor.execute(sql, (key, url))
                    
                    # Insert or update sources table with the local UUID
                    sql = """
                        INSERT INTO sources (`annex-key`, sources)
                        VALUES (%s, %s) as new(new_key, new_sources)
                        ON DUPLICATE KEY UPDATE
                        sources = JSON_MERGE_PATCH(
                            sources, new_sources);
                    """
                    path = f'$."{self.local_uuid}"'
                    cursor.execute(sql, (key, f'{{"{self.local_uuid}":1}}'))
                    
                    # Commit transaction
                    connection.commit()
                    
            except pymysql.Error as e:
                connection.rollback()
                raise e

    def process_url(self, url: str) -> Optional[str]:
        """Process a single URL: download, compute key, register, and reinject"""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            try:
                # Download the file
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                temp_file.flush()
                
                # Compute the key that git-annex will use
                key = self.compute_annex_key(temp_file.name)
                
                # Register the URL
                self.register_url(key, url)
                
                # Reinject the file
                self.reinject_file(key, temp_file.name)
                
                return key
                
            except (requests.RequestException, subprocess.CalledProcessError, ValueError) as e:
                print(f"Error processing URL {url}: {str(e)}")
                return None

    @logger.method
    def process_batch(self):
        """Process a batch of unprocessed URLs"""
        urls = list(self.fetch_unprocessed_urls())
        logger.log(f"Discovered {len(urls)} unprocessed URLs: {urls}")
        
        for url in urls:
            with logger.section(f"processing {url}"):
                key = self.process_url(url)
                
                if key:
                    try:
                        self.update_database(url, key)
                        print(f"Successfully processed {url} with key {key}")
                    except pymysql.Error as e:
                        print(f"Database error for URL {url}: {str(e)}")

    def parse_log_file(self, content: str) -> Set[str]:
        """Parse a .log file and return a set of UUIDs that have the file"""
        uuids = set()
        for line in content.splitlines():
            if not line.strip():
                continue
            try:
                timestamp, present, uuid = line.split()
                if present == "1":
                    uuids.add(uuid)
            except ValueError:
                print(f"Warning: malformed log line: {line}")
        return uuids

    def parse_web_log(self, content: str) -> List[str]:
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

    def get_file_from_git(self, ref: str, path: str) -> Optional[str]:
        """Get file content from git"""
        try:
            return self.git.run('annex', 'show', f'{ref}:{path}')
        except subprocess.CalledProcessError:
            return None

    def update_sources_table(self, key: str, uuids: Set[str]):
        """Update the sources table with the UUIDs that have the key"""
        if not uuids:
            return

        sources_json = {uuid: True for uuid in uuids}
        with self.db_connection() as connection:
            with connection.cursor() as cursor:
                sql = """
                    INSERT INTO sources (`annex-key`, sources)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    sources = %s
                """
                sources_str = json.dumps(sources_json)
                num_sources = len(uuids)
                cursor.execute(sql, (key, sources_str, num_sources, sources_str, num_sources))
                connection.commit()

    def update_annex_keys_table(self, key: str, urls: List[str]):
        """Update the annex-keys table with URLs for the key"""
        if not urls:
            return

        with self.db_connection() as connection:
            with connection.cursor() as cursor:
                # Insert all URLs for this key
                sql = """
                    INSERT INTO `annex-keys` (url, `annex-key`)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    `annex-key` = VALUES(`annex-key`)
                """
                cursor.executemany(sql, [(url, key) for url in urls])
                connection.commit()

    def flush_sources_batch(self, cursor, sources_batch):
        """Execute a batch of source updates"""
        if not sources_batch:
            return

        
        sql = f"""
            INSERT INTO sources (`annex-key`, sources)
            VALUES (%s, %s) AS new(new_key, new_sources)
            ON DUPLICATE KEY UPDATE
            sources = JSON_MERGE_PATCH(sources, new_sources);
        """
        
        # Prepare all parameters
        params = []
        for entry in sources_batch:
            # Parameters for INSERT
            print(entry)
            initial_json = json.dumps({uuid: 1 for uuid in entry['uuid_dict']})
            params.append([entry['key'], initial_json])
            
        print(sql, params)
        cursor.executemany(sql, params)

    def flush_urls_batch(self, cursor, urls_batch):
        """Execute a batch of URL updates"""
        if not urls_batch:
            return
            
        sql = """
            INSERT INTO `annex-keys` (url, `annex-key`)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
            `annex-key` = VALUES(`annex-key`)
        """
        cursor.executemany(sql, urls_batch)

    def discover_and_populate(self):
        """Walk the git-annex branch and populate the database"""
        print("Starting repository discovery...")
        
        # Stream the git ls-tree output
        process = subprocess.Popen(
            ['git', '-C', self.git_annex_path, 'ls-tree', '-r', 'git-annex', '--name-only'],
            stdout=subprocess.PIPE,
            text=True
        )

        sources_batch = []
        urls_batch = []
        
        with self.db_connection() as connection:
            with connection.cursor() as cursor:
                # Process files as they come in
                for line in process.stdout:
                    filename = line.strip()
                    if '/' not in filename:
                        continue
                    if filename.endswith('.log'):
                        key = os.path.splitext(os.path.basename(filename))[0]
                        print(f"Processing key: {key}")
                        
                        # Get location log
                        log_content = self.get_file_from_git('git-annex', filename)
                        if log_content:
                            uuid_dict = self.parse_log_file(log_content)
                            if uuid_dict:
                                sources_batch.append({
                                    'key': key,
                                    'uuid_dict': uuid_dict
                                })
                            
                            if len(sources_batch) >= self.db_batch_size:
                                self.flush_sources_batch(cursor, sources_batch)
                                connection.commit()
                                sources_batch = []
                        
                        # Check for web URLs
                        web_log = f"{os.path.splitext(filename)[0]}.log.web"
                        web_content = self.get_file_from_git('git-annex', web_log)
                        if web_content:
                            urls = self.parse_web_log(web_content)
                            urls_batch.extend((url, key) for url in urls)
                            
                            if len(urls_batch) >= self.db_batch_size:
                                self.flush_urls_batch(cursor, urls_batch)
                                connection.commit()
                                urls_batch = []
                
                # Flush any remaining batches
                if sources_batch:
                    self.flush_sources_batch(cursor, sources_batch)
                if urls_batch:
                    self.flush_urls_batch(cursor, urls_batch)
                connection.commit()

        # Make sure the process completed successfully
        retcode = process.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(retcode, 'git ls-tree')

def main():
    _, git_path = sys.argv
    # Configuration
    git = Git(git_path)
    local_uuid = git.config['annex.uuid']
    logger.log(f"Local UUID: {local_uuid}")
    downloader = GitAnnexDownloader(
        git = git,
        local_uuid = local_uuid,  # Replace with your local repo's UUID
        batch_size=10
    )
    
    # Choose mode:
    # downloader.process_batch()  # Process new URLs
    downloader.discover_and_populate()  # Populate from existing repo

if __name__ == "__main__":
    main()