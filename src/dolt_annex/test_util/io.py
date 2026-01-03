#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilities for managing IO during tests.
"""

from typing_extensions import TextIO


class Tee(TextIO):
    """
    A text file object that writes to multiple outputs.
    
    Most commonly used to split output between stdout and a file.
    """

    def __init__(self, *streams: TextIO):
        self.streams = streams

    def write(self, s: str) -> int:
        for stream in self.streams:
            stream.write(s)
        return len(s)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()
