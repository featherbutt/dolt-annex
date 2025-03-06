from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Literal, Optional

from bup.repo.base import RepoProtocol as Repo

"""A FilePatch is a function that describes how to modify a file."""
FilePatch = Callable[[Optional[bytes]], bytes]

def new_file(contents: bytes):
    """Return a FilePatch that creates a new file with the given contents."""
    def inner(old_contents: Optional[bytes]):
        assert old_contents is None
        return contents
    return inner

def append_file(contents: bytes):
    """Return a FilePatch that appends the given contents to a file."""
    def inner(old_contents: Optional[bytes]):
        if old_contents is None:
            return contents
        return old_contents + contents
    return inner
    
def update_file(contents: List[List[bytes]], key_index: int, delimieter: bytes = b' '):
    """Return a FilePatch that updates a row in a file of delimiter-separated values."""
    keys = [content[key_index] for content in contents]
    def inner(old_contents: Optional[bytes]):
        if old_contents is None:
            return b'\n'.join(delimieter.join(line) for line in contents)
        out_lines = []
        for line in old_contents.split(b'\n'):
            parts = line.split(delimieter)
            if len(parts) > key_index and parts[key_index] not in keys:
                out_lines.append(line)
        out_lines.extend(delimieter.join(line) for line in contents)
        return b'\n'.join(out_lines)
    return inner
@dataclass
class DirectoryPatch:
    """An object describing a patch to be applied to a git branch."""
    files: Dict[bytes, DirectoryPatch | FilePatch]

    def __init__(self):
        self.files = {}

    def __bool__(self):
        return bool(self.files)
    
    def apply(self, repo: Repo, path: bytes):
        """Apply the patch to the given repository at the given path."""

    def insert(self, path: bytes, patch: DirectoryPatch | FilePatch):
        """Insert a patch at the given path."""
        head, *tail = path.split(b'/', 1)
        if not tail:
            self.files[head] = patch
        else:
            if head not in self.files:
                self.files[head] = DirectoryPatch()
            self.files[head].insert(tail[0], patch)


    

