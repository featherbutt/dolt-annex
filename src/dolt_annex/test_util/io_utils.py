#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities for managing IO during tests.
"""
from contextlib import _RedirectStream
from io import BytesIO
from typing import BinaryIO, override
from typing_extensions import TextIO

class BufferStringIO(TextIO):
    """
    A text file object that writes to an in-memory bytes buffer.
    
    Unlike io.StringIO, this class has an underlying BytesIO buffer, which
    some libraries expect.
    """

    _buffer: BytesIO
    _encoding: str

    def __init__(self) -> None:
        self._buffer = BytesIO()
        self._encoding = 'utf-8'

    @property
    @override
    def buffer(self) -> BytesIO:
        return self._buffer
    
    @property
    @override
    def encoding(self) -> str:
        return self._encoding
    
    @override
    def write(self, s: str) -> int:
        self.buffer.write(s.encode('utf-8'))
        return len(s)

    def getvalue(self) -> str:
        return self._buffer.getvalue().decode('utf-8')

class BinaryTee(BinaryIO):
    """
    A binary file object that writes to multiple outputs.
    
    Most commonly used to split output between stdout and a file.
    """

    def __init__(self, *streams: BinaryIO):
        self.streams = streams

    @override
    def write(self, s: bytes) -> int:
        for stream in self.streams:
            stream.write(s)
        return len(s)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()

class TextTee(TextIO):
    """
    A text file object that writes to multiple outputs.
    
    Most commonly used to split output between stdout and a file.
    """
    _buffer: BinaryTee

    def __init__(self, *streams: TextIO):
        self.streams = streams
        self._buffer = BinaryTee(*(stream.buffer for stream in streams))

    @property
    @override
    def buffer(self) -> BinaryIO:
        return self._buffer

    def write(self, s: str) -> int:
        for stream in self.streams:
            stream.write(s)
        return len(s)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()

class redirect_stdin(_RedirectStream):
    """
    Context manager for temporarily redirecting stdin from another file.
    """

    _stream = "stdin"