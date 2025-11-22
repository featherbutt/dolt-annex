#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from typing import ClassVar
from pydantic import BaseModel
from typing_extensions import Self, Optional

def Loadable(extension: str, config_dir = Path(".")):
    """
    Create a mixin class that allows loading from a JSON file.
    """
    class LoadableMixin(BaseModel):
        """
        A mixin class that allows loading from a JSON file.
        """

        name: str

        cache: ClassVar[dict[str, Self]] = {}

        def __init__(self, **data):
            super().__init__(**data)
            self.cache[self.name] = self

        @classmethod
        def load(cls, name: str) -> Optional[Self]:
            """
            Returns an instance loaded from a JSON file, or None if the file does not exist.
            """
            if name in cls.cache:
                return cls.cache[name]
            
            path = config_dir / f"{name}.{extension}"
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                    if data.get("name") != name:
                        raise ValueError(f"Table name {data.get('name')} does not match expected name {name}")
                    instance = cls(**data)
                    cls.cache[name] = instance
                    return instance
            return None
        
        @classmethod
        def must_load(cls, name: str) -> Self:
            """
            Returns an instance loaded from a JSON file, or raise ValueError if the file does not exist.
            """
            result = cls.load(name)
            if not result:
                raise ValueError(f"Could not load {name}")
            return result
        
        def save_as(self, name: str):
            """
            Saves the instance to a JSON file.
            """
            path = config_dir / f"{name}.{extension}"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self.model_dump_json(), f, ensure_ascii=False, indent=4)

    return LoadableMixin
