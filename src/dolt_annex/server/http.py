#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Serves files from a FileStore over a local HTTP server.

Each file is available at `GET /<file key>` on the server, so a file's URL is
`http://127.0.0.1:<port>/<file key>`.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import mimetypes
import webbrowser

from aiohttp import web

from dolt_annex.filestore.base import FileStore
from dolt_annex.file_keys.base import FileKey

READ_BUFFER_SIZE = 1024 * 1024  # 1 MB

class PendingFiles:
    """
    Tracks files requested by the program but not yet served.

    This is useful for ensuring that a local server does not close until all requested files have been served.
    """
    condition: asyncio.Condition
    files: set[FileKey]

    def __init__(self) -> None:
        self.condition = asyncio.Condition()
        self.files = set()

    async def wait_for_file(self, file_key: FileKey):
        self.files.add(file_key)
        async with self.condition:
            await self.condition.wait_for(lambda: file_key not in self.files)

    async def mark_file_done(self, file_key: FileKey):
        self.files.discard(file_key)
        async with self.condition:
            self.condition.notify_all()


def content_type_for(file_key: FileKey) -> str:
    extension = file_key["extension"]
    if extension:
        guessed, _ = mimetypes.guess_type(f"file.{extension}")
        if guessed:
            return guessed
    return "application/octet-stream"

def make_app(filestore: FileStore, pending_files: PendingFiles) -> web.Application:
    async def get_file(request: web.Request) -> web.StreamResponse:

        file_key = FileKey.try_parse(request.match_info["file_key"].encode("utf-8"))
        if file_key is None:
            raise web.HTTPBadRequest(text="Not a valid file key.")
        if not await filestore.exists(file_key):
            raise web.HTTPNotFound(text="File not found.")

        response = web.StreamResponse(headers={"Content-Type": content_type_for(file_key)})
        async with filestore.with_file_object(file_key) as f:
            await response.prepare(request)
            while buf := await f.read(READ_BUFFER_SIZE):
                await response.write(buf)
        await response.write_eof()
        await pending_files.mark_file_done(file_key)
        return response

    app = web.Application()
    app.router.add_get("/{file_key}", get_file)
    return app

@asynccontextmanager
async def serve_filestore(filestore: FileStore, *, host: str = "127.0.0.1", port: int = 0) -> AsyncGenerator[HTTPServer]:
    """
    Run a local web server for the filestore while the context is active.

    Yields the base URL of the server, e.g. `http://127.0.0.1:54321`. Port 0 picks a free port.
    """
    pending_files = PendingFiles()
    runner = web.AppRunner(make_app(filestore, pending_files))
    await runner.setup()
    try:
        site = web.TCPSite(runner, host, port)
        await site.start()
        bound_port = runner.addresses[0][1]
        yield HTTPServer(base_url=f"http://{host}:{bound_port}", pending_files=pending_files)
    finally:
        await runner.cleanup()

@dataclass
class HTTPServer:

    base_url: str
    pending_files: PendingFiles

    async def display_file(self, queried_key: FileKey):
        webbrowser.open(f"{self.base_url}/{queried_key}")
        await self.pending_files.wait_for_file(queried_key)
