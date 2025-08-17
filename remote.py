#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextvars import ContextVar
from dataclasses import dataclass
import json
import os
from typing_extensions import Dict
from uuid import UUID

@dataclass
class Remote:
    name: str
    uuid: UUID
    url: str

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
