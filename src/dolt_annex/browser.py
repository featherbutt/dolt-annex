"""
Module for displaying files from the filestore using the default system web browser.

Eventually this will run a local web server to display files. Right now it just writes to temporary files.
"""

import tempfile

from InquirerPy.inquirer import text

from dolt_annex.filestore.base import FileStore
from dolt_annex.file_keys.base import FileKey

async def display_file(filestore: FileStore, queried_key: FileKey):
    async with filestore.with_file_object(queried_key) as f:
        with tempfile.NamedTemporaryFile(
            suffix="." + queried_key["extension"], mode="wb"
        ) as temp_file:
            buffer_size = 1024 * 1024  # 1 MB buffer size
            while True:
                buf = await f.read(buffer_size)
                if not buf:
                    break
                temp_file.write(buf)
            temp_file.flush()
            temp_path = temp_file.name

            import webbrowser
            webbrowser.open("file://" + temp_path)
            await text(
                message="Press Enter to continue...",
            ).execute_async()