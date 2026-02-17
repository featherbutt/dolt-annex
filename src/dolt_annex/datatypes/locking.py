from collections.abc import Generator
from contextlib import contextmanager
from typing import Dict, Protocol

from filelock import FileLock, Timeout as FileLockTimeout
import fs.osfs
import fs.memoryfs

from dolt_annex.datatypes.async_types import ContextManager
from dolt_annex.datatypes.file_io import Path


class FailedToAcquireLock(Exception):
    pass

class LockManager(Protocol):
    """
    Protocol for locking access to file system resources.
    
    The lock function attempts to acquire a lock for the given key without blocking.
    If the lock cannot be acquired, it raises a FailedToAcquireLock exception.
    """

    def lock(self, lock_key: str) -> ContextManager[None]:
        ...


class FileSystemLockManager:
    """Lock manager using filelock` for real filesystems."""

    def __init__(self, locks_dir: Path):
        self.locks_dir = locks_dir

    @contextmanager
    def lock(self, lock_key: str) -> Generator[None]:
        lock_path = self.locks_dir / f"{lock_key}.lock"
        lock_syspath = lock_path.getsyspath()
        assert lock_syspath is not None, "FileSystemLockManager requires a filesystem with getsyspath() support"
        lock = FileLock(lock_syspath, blocking=False)
        try:
            with lock:
                yield
        except FileLockTimeout:
            raise FailedToAcquireLock(lock_key) from None

class InMemoryLockManager:
    """
    Lock manager for in-memory filesystems using a simple dictionary.
    
    This is not thread-safe, but is safe to use in single-threaded async code.
    """

    def __init__(self):
        self._locks: Dict[str, bool] = {}

    @contextmanager
    def lock(self, lock_key: str) -> Generator[None]:
        if self._locks.get(lock_key, False):
            raise FailedToAcquireLock(lock_key) from None
        
        self._locks[lock_key] = True
        try:
            yield
        finally:
            self._locks[lock_key] = False

def new_lock_manager(locks_dir: Path) -> LockManager:
    """
    Factory function to create a LockManager based on the type of filesystem.
    """
    match locks_dir.fs:
        case fs.memoryfs.MemoryFS():
            return InMemoryLockManager()
        case fs.osfs.OSFS():
            return FileSystemLockManager(locks_dir)
        case _:
            raise ValueError("Unsupported filesystem type for ArchiveFS")
