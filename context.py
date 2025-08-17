#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from contextvars import ContextVar
from typing_extensions import Dict, TypeVar

from type_hints import UUID, PathLike

T = TypeVar('T')

@contextmanager
def assign(key: ContextVar[T], value: T):
    token = key.set(value)
    try:
        yield
    finally:
        key.reset(token)

local_uuid = ContextVar[UUID]('local_uuid')
