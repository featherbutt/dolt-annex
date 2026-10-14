from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from copy import deepcopy
import json
import pathlib
from typing import Dict

import pyjson5

type JSON = JsonPrimitive | JsonObject | JsonArray
type JsonPrimitive = str | int | float | bool | None
type JsonObject = Dict[str, JSON]
type JsonArray = list[JSON]

@asynccontextmanager
async def open_json(path: pathlib.Path) -> AsyncGenerator[JsonObject, None]:
    """
    Open an deserialize a JSON5 file at the given path. When the file is closed,
    any changes made to the JSON data will be written back to the file.
    """
    # TODO: Use a file lock to prevent concurrent modifications.
    with path.open("r+") as f:
        data_bytes = f.read()
        if data_bytes:
            data = pyjson5.loads(data_bytes)
        else:
            data = {}
        original_data = deepcopy(data)
        try:
            yield data
        finally:
            if data != original_data:
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()