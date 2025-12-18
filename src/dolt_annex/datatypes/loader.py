#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
import json
import pathlib
from typing import ClassVar
from pydantic import BaseModel
from typing_extensions import Self, Optional

registered_subclasses: set[type['Loadable']] = set()

class Loadable(BaseModel):
    """
    A class that can be loaded from a JSON file.

    All loaded or instantiated instances are cached in memory by name.
    """

    extension: ClassVar[str]
    config_dir: ClassVar[pathlib.Path]
    cache: ClassVar[ContextVar[dict[str, Self]]]

    def __init_subclass__(cls, extension: str, config_dir: pathlib.Path, **kwargs):
        cls.extension = extension
        cls.config_dir = config_dir
        cls.cache = ContextVar(f"{cls.__name__}_cache", default={})
        registered_subclasses.add(cls)
        return super().__init_subclass__(**kwargs)

    @classmethod
    @contextmanager
    def context(cls):
        """
        A contect manager that reverts the cache on exit.

        If called on a subclass, only reverts that subclass's cache.
        """
        if cls is Loadable:
            with ExitStack() as stack:
                for subclass in registered_subclasses:
                    stack.enter_context(subclass.context())
                yield
            return
        token = cls.cache.set(cls.cache.get().copy())
        try:
            yield
        finally:
            cls.cache.reset(token)

    name: str

    def __init__(self, **data):
        super().__init__(**data)
        cache = self.cache.get()
        cache[self.name] = self

    @classmethod
    def load(cls, name: str) -> Optional[Self]:
        """
        Returns an instance loaded from a JSON file, or None if the file does not exist.
        """
        cache = cls.cache.get()
        if name in cache:
            return cache[name]
        
        path = cls.config_dir / f"{name}.{cls.extension}"
        if path.exists():
            with path.open() as f:
                data = pyjson5.load(f)
                if not isinstance(data, dict):
                    raise ValueError(f"{cls} loaded from {path} is not an object")
                if data.get("name") != name:
                    raise ValueError(f"{cls} name {data.get('name')} does not match expected name {name}")
                instance = cls(**data)
                cache[name] = instance
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
    
    def save(self):
        """
        Saves the instance to a JSON file.
        """
        path = self.config_dir / f"{self.name}.{self.extension}"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            f.write(self.model_dump_json(ensure_ascii=False, indent=4).encode("utf-8"))
