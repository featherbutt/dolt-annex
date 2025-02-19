import os
import tempfile
import subprocess
import hashlib
import requests
import pymysql
from typing import Optional, Tuple
from contextlib import contextmanager

class GitAnnexDownloader:
    def __init__(self, db_host: str, db_user: str, db_password: str, db_name: str, 
                 git_annex_path: str, batch_size: int = 10):
        self.db_config = {
            'host': db_host,
            'user': db_user,
            'password': db_password,
            'db': db_name,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.git_annex_path = git_annex_path
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
        """Update the database with the new key for a URL"""
        with self.db_connection() as connection:
            with connection.cursor() as cursor:
                sql = """
                    UPDATE `annex-keys` 
                    SET annex_key = %s 
                    WHERE url = %s
                """
                cursor.execute(sql, (key, url))
                connection.commit()

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

def main():
    # Configuration
    downloader = GitAnnexDownloader(
        db_host="localhost",
        db_user="your_user",
        db_password="your_password",
        db_name="your_database",
        git_annex_path="/path/to/git/annex/repo",
        batch_size=10
    )
    
    # Process a batch of URLs
    downloader.process_batch()

if __name__ == "__main__":
    main()