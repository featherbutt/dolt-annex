#!/usr/bin/env python
# -*- coding: utf-8 -*-

from contextlib import ExitStack, contextmanager
from typing_extensions import Optional, ContextManager, Generator, Self, override, Any, BinaryIO

from dolt_annex.file_keys import FileKey

from .base import FileStore, YesNoMaybe

class Union(FileStore):
    """
    A filestore that combines multiple underlying filestores together.

    New files are written to the first filestore, while reads check each filestore in order.
    """

    children: list[FileStore]

    @override
    def put_file_object(self, in_fd: BinaryIO, file_key: Optional[FileKey] = None) -> None:
        """Upload a file-like object to the remote. If file_key is not provided, it will be computed."""
        return self.children[0].put_file_object(in_fd, file_key)

    @override
    def get_file_object(self, file_key: FileKey) -> ContextManager[BinaryIO]:
        """Get a file-like object for a file in the remote by its key."""
        return self.children[0].get_file_object(file_key)

    @override
    def exists(self, file_key: FileKey) -> bool:
        """
        Returns whether the key exists in the filestore.
        """
        for child in self.children:
            match child.possibly_exists(file_key):
                case YesNoMaybe.YES:
                    return True
                case YesNoMaybe.MAYBE:
                    if child.exists(file_key):
                        return True
                case YesNoMaybe.NO:
                    continue
        return False

    def possibly_exists(self, file_key: FileKey) -> YesNoMaybe:
        """
        Returns Yes if the file definitely exists in the filestore,
        No if the file definitely does not exist in the filestore,
        and Maybe if the file might exist in the filestore.

        This is often more efficient than calling exists.

        For instance, filestores that use a bloom filter can quickly
        return No or Maybe, at the cost of never returning Yes.
        """
        for child in self.children:
            match child.possibly_exists(file_key):
                case YesNoMaybe.YES:
                    return YesNoMaybe.YES
                case YesNoMaybe.MAYBE:
                    return YesNoMaybe.MAYBE
                case YesNoMaybe.NO:
                    continue
        return YesNoMaybe.NO
    
    @override
    def flush(self) -> None:
        """Flush any pending operations to the filestore."""
        for child in self.children:
            child.flush()


    @override
    def open(self, config: Any) -> ContextManager[None]:
        """Open the filestore, loading or initializing metrics tracking."""
        @contextmanager
        def inner(self: Self) -> Generator[None]:
            with ExitStack() as stack:
                for child in self.children:
                    stack.enter_context(child.open(config))
                yield

                self.flush()

        return inner(self)