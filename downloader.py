import os
import tempfile
import subprocess
import hashlib
import requests
import pymysql
import json
from typing import Optional, Tuple, Dict, Set, List
from contextlib import contextmanager

class GitAnnexDownloader:
    def __init__(self, db_host: str, db_user: str, db_password: str, db_name: str, 
                 git_annex_path: str, local_uuid: str, batch_size: int = 10):
        self.db_config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'db': db_name,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.git_annex_path = git_annex_path
        self.local_uuid = local_uuid
        self.batch_size = batch_size

    @contextmanager
    def db_connection(self):
        """Context manager for database connections"""
        connection = pymysql.connect(**self.db_config)
        try:
            yield connection
        finally:
            connection.close()

    def fetch_unprocessed_urls(self) -> list[dict]:
        """Fetch a batch of URLs that haven't been processed yet"""
        with self.db_connection() as connection:
            with connection.cursor() as cursor:
                sql = """
                    SELECT url 
                    FROM `annex-keys` 
                    WHERE annex_key IS NULL 
                    LIMIT %s
                """
                cursor.execute(sql, (self.batch_size,))
                return cursor.fetchall()

    def compute_annex_key(self, file_path: str) -> str:
        """Compute the git-annex key for a file without adding it to the repository"""
        result = subprocess.run(
            ['git', '-C', self.git_annex_path, 'annex', 'calckey', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()

    def register_url(self, key: str, url: str):
        """Register a URL for a key in git-annex"""
        subprocess.run(
            ['git', '-C', self.git_annex_path, 'annex', 'registerurl', key, url],
            check=True
        )

    def reinject_file(self, file_path: str) -> str:
        """Reinject a file into git-annex and return its key"""
        result = subprocess.run(
            ['git', '-C', self.git_annex_path, 'annex', 'reinject', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        # Extract key from reinject output
        return result.stdout.strip().split()[0]

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
                        SET annex_key = %s 
                        WHERE url = %s
                    """
                    cursor.execute(sql, (key, url))
                    
                    # Insert or update sources table with the local UUID
                    sql = """
                        INSERT INTO sources (annex_key, sources, numSources)
                        VALUES (%s, %s, 1)
                        ON DUPLICATE KEY UPDATE
                        sources = JSON_SET(
                            COALESCE(sources, '{}'),
                            %s,
                            'true'
                        ),
                        numSources = JSON_LENGTH(
                            JSON_SET(
                                COALESCE(sources, '{}'),
                                %s,
                                'true'
                            )
                        )
                    """
                    path = f'$."{self.local_uuid}"'
                    cursor.execute(sql, (key, f'{{{self.local_uuid}:true}}', path, path))
                    
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
                actual_key = self.reinject_file(temp_file.name)
                
                # Verify keys match
                if key != actual_key:
                    raise ValueError(f"Computed key {key} doesn't match actual key {actual_key}")
                
                return key
                
            except (requests.RequestException, subprocess.CalledProcessError, ValueError) as e:
                print(f"Error processing URL {url}: {str(e)}")
                return None
            finally:
                # Clean up temporary file
                os.unlink(temp_file.name)

    def process_batch(self):
        """Process a batch of unprocessed URLs"""
        urls = self.fetch_unprocessed_urls()
        
        for url_data in urls:
            url = url_data['url']
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
                if present != "1":
                    print(f"Found unexpected present value in {content}")
                else:
                    urls.append(url)
            except ValueError:
                print(f"Warning: malformed web log line: {line}")
        return urls

    def get_file_from_git(self, ref: str, path: str) -> Optional[str]:
        """Get file content from git"""
        try:
            result = subprocess.run(
                ['git', '-C', self.git_annex_path, 'show', f'{ref}:{path}'],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
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
                    INSERT INTO sources (annex_key, sources, numSources)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    sources = %s,
                    numSources = %s
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
                    INSERT INTO `annex-keys` (url, annex_key)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                    annex_key = VALUES(annex_key)
                """
                cursor.executemany(sql, [(url, key) for url in urls])
                connection.commit()

    def discover_and_populate(self):
        """Walk the git-annex branch and populate the database"""
        print("Starting repository discovery...")
        
        # Get list of all files in git-annex branch
        result = subprocess.run(
            ['git', '-C', self.git_annex_path, 'ls-tree', '-r', 'git-annex', '--name-only'],
            capture_output=True,
            text=True,
            check=True
        )
        
        all_files = result.stdout.splitlines()
        log_files = [f for f in all_files if f.endswith('.log')]
        
        for log_file in log_files:
            key = os.path.splitext(os.path.basename(log_file))[0]
            print(f"Processing key: {key}")
            
            # Get location log
            log_content = self.get_file_from_git('git-annex', log_file)
            if log_content:
                uuids = self.parse_log_file(log_content)
                self.update_sources_table(key, uuids)
            
            # Check for web URLs
            web_log = f"{os.path.splitext(log_file)[0]}.log.web"
            web_content = self.get_file_from_git('git-annex', web_log)
            if web_content:
                urls = self.parse_web_log(web_content)
                self.update_annex_keys_table(key, urls)

def main():
    # Configuration
    downloader = GitAnnexDownloader(
        db_host="localhost",
        db_user="your_user",
        db_password="your_password",
        db_name="your_database",
        git_annex_path="/path/to/git/annex/repo",
        local_uuid="00000000-0000-0000-0000-000000000000",  # Replace with your local repo's UUID
        batch_size=10
    )
    
    # Choose mode:
    # downloader.process_batch()  # Process new URLs
    downloader.discover_and_populate()  # Populate from existing repo

if __name__ == "__main__":
    main()