#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import json
import os
from pathlib import Path
from uuid import UUID

@dataclass
class Remote:
    name: str
    uuid: UUID
    url: str

    def files_dir(self) -> Path:
        if self.url.startswith("file://"):
            return Path(self.url[7:])
        elif self.url.startswith("ssh://"):
            return Path(self.url.split(":", 2)[2])
        else:
            return Path(self.url)

    @staticmethod
    def from_name(name: str):
        # Look for a file in the current directory that matches the name.
        path = f"{name}.remote"
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
                return Remote(
                    name=name,
                    uuid=data["uuid"],
                    url=data["url"],
                )
        return None
