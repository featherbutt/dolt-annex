#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from collections.abc import Iterable
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
import pathlib
from typing import Annotated, Any
from pydantic import ModelWrapValidatorHandler, SerializerFunctionWrapHandler, WrapSerializer, model_serializer, model_validator
import pyjson5
from typing_extensions import Self, Optional, ClassVar

from dolt_annex.datatypes.pydantic import StrictBaseModel

registered_subclasses: set[type[Loadable]] = set()

class Loadable(StrictBaseModel):
    """
    A class that can be loaded from a JSON file.

    All loaded or instantiated instances are cached in memory by name.
    """

    extension: ClassVar[str]
    config_dir: ClassVar[pathlib.Path]
    cache: ClassVar[ContextVar[dict[str, Self]]]

    def __init_subclass__(cls, extension: str | None = None, config_dir: pathlib.Path | None = None, **kwargs):
        if Loadable in cls.__bases__:
            if extension is None or config_dir is None:
                raise ValueError("Direct subclasses of Loadable must specify 'extension' and 'config_dir'")
            cls.extension = extension
            cls.config_dir = config_dir
            cls.cache = ContextVar(f"{cls.__name__}_cache", default={})
            registered_subclasses.add(cls)
        return super().__init_subclass__(**kwargs)

    @classmethod
    @contextmanager
    def context(cls):
        """
        A context manager that reverts the cache on exit.

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

    name: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        cache = self.cache.get()
        if self.name is not None:
            cache[self.name] = self

    @classmethod
    def load(cls, name: str) -> Optional[Self]:
        """
        Returns an instance loaded from a JSON file, or None if the file does not exist.
        """
        cache = cls.cache.get()
        if name in cache:
            return cache[name]

        data = cls.load_dict(name)
        if data is None:
            return None

        instance = cls(**data)
        cache[name] = instance
        return instance

    @classmethod
    def load_dict(cls, name: str) -> dict | None:
        """
        Returns an instance loaded from a JSON file, or None if the file does not exist.
        """
        
        path = cls.config_dir / f"{name}.{cls.extension}"
        if path.exists():
            with path.open() as f:
                try:
                    data = pyjson5.load(f)
                except pyjson5.Json5Exception as e:
                    raise ValueError(f"Error loading {name} from {path}: {e}")
                if not isinstance(data, dict):
                    raise ValueError(f"{cls} loaded from {path} is not an object")
                if data.get("name") != name:
                    raise ValueError(f"{cls} name {data.get('name')} does not match expected name {name}")
                return data
        return None
    
    @classmethod
    def all(cls) -> Iterable[Self]:
        """Returns all configuration objects of this type."""
        if cls.config_dir.exists():
            for file in cls.config_dir.iterdir():
                if '.' not in file.name:
                    continue
                name, extension = file.name.split('.', 1)
                if extension == cls.extension:
                    cls.load(name)
        return cls.cache.get().values()

    @classmethod
    def must_load(cls, name: str) -> Self:
        """
        Returns an instance loaded from a JSON file, or raise ValueError if the file does not exist.
        """
        result = cls.load(name)
        if not result:
            raise ValueError(f"Could not load {name}")
        return result

    @classmethod
    def must_load_dict(cls, name: str) -> dict:
        """
        Returns an instance loaded from a JSON file, or raise ValueError if the file does not exist.
        """
        result = cls.load_dict(name)
        if not result:
            raise ValueError(f"Could not load {name}")
        return result

    @model_validator(mode='wrap')
    @classmethod
    def validate_loadable(cls, value: Any, handler: ModelWrapValidatorHandler[Self]) ->Self:
        if isinstance(value, str):
            cache = cls.cache.get()
            if value in cache:
                return handler(cache[value])
            return handler(cls.must_load_dict(value))
        return handler(value)
    
    def save(self):
        """
        Saves the instance to a JSON file.
        """
        if self.name is None:
            raise ValueError("Cannot save an instance without a name.")
        path = self.config_dir / f"{self.name}.{self.extension}"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            f.write(self.model_dump_json(ensure_ascii=False, indent=4).encode("utf-8"))

def serialize_loadable(
    self, handler: SerializerFunctionWrapHandler
) -> Any:
    if self.name is not None:
        return self.name
    return handler(self)

type Named[T: Loadable] = Annotated[T, WrapSerializer(serialize_loadable)]