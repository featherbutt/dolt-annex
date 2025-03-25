#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module applies a patch to an existing git branch,
and writes any new git objects to a packfile.

This is a modified version of the `bup save` command.

It uses bup's bloom filter in order to avoid writing objects that already exist in the branch.
"""

from binascii import hexlify, unhexlify
from dataclasses import dataclass
import os
import stat
import time
from typing import Optional

from bup import metadata
from bup.hashsplit import GIT_MODE_FILE, GIT_MODE_SYMLINK, GIT_MODE_TREE
from bup.helpers import path_components
from bup.repo.base import RepoProtocol as Repo
from bup.git import tree_decode
from bup.index import IX_HASHVALID, IX_EXISTS, IX_SHAMISSING, Entry as BupEntry
from bup.tree import Stack, _write_tree

from logger import logger

from bup_ext.patch import DirectoryPatch

log = logger.info

GIT_MODE_FILE = 0o100644
GIT_MODE_TREE = 0o40000
GIT_MODE_SYMLINK = 0o120000

def pop(stack: Stack, repo: Repo, override_tree = None):
    """
    Pop the current directory from the stack and write it to the repo.
    
    Unlike calling Stack.pop, this function won't add bup metadata.
    """
    tree = stack.stack.pop()
    if not override_tree:
        items = stack._clean(tree)
        tree_oid = _write_tree(repo, tree.meta, items, add_meta=False)
    else:
        tree_oid = override_tree
    if len(stack):
        stack.append_to_current(tree.name, GIT_MODE_TREE, GIT_MODE_TREE,
                                tree_oid, None)
    return tree_oid

class NewDirectoryEntry(BupEntry):
    """This entry describes a directory that is being inserted into the tree."""
    def __init__(self, repo, basename: bytes, name: bytes, mode, patch: DirectoryPatch):
        BupEntry.__init__(self, basename, name, None, None)
        self.repo = repo
        self.patch = patch
        self.mode = mode
        self.gitmode = mode
        self.flags = IX_SHAMISSING
        self.ref = None
        self.sha = None

    def iter(self, name=None, wantrecurse=None):
        """Walk the directory structure, yielding a BupEntry for each child."""
        dname = name
        if dname and not dname.endswith(b'/'):
            dname += b'/'

        for name, file in self.patch.files.items():
            if not callable(file):
                child = NewDirectoryEntry(self.repo, name, self.name + name + b'/', GIT_MODE_TREE, file)
                yield from child.iter(name=name, wantrecurse=wantrecurse)
                yield child
            else:
                contents = file(None)
                new_oid = self.repo.write_data(contents)
                child = GitEntry(self.repo, self, None, name, self.name + name, GIT_MODE_FILE, hexlify(new_oid))
                yield child

class GitEntry(BupEntry):
    """This entry describes a file or directory in a git repository."""
    def __init__(self, repo: Repo, parent: Optional['GitEntry'], additional_files: DirectoryPatch, basename, name, mode, ref: bytes):
        BupEntry.__init__(self, basename, name, None, None)
        self.repo = repo
        self.parent = parent
        self.additional_files = additional_files
        self.ref = ref
        self.sha = unhexlify(ref)
        self.flags = IX_HASHVALID|IX_EXISTS
        self.mode = mode
        self.gitmode = mode

    def invalidate(self):
        self.flags &= ~IX_HASHVALID
        self.sha = None
        self.ref = None
        if self.parent:
            self.parent.invalidate()

    def repack(self):
        pass

    def iter(self):
        ref = self.ref

        item_it = self.repo.cat(ref)
        get_oidx, typ, _ = next(item_it)
        assert get_oidx == ref

        data = b''.join(item_it)

        if typ != b'tree':
            assert typ == b'blob'
            return

        if self.additional_files:
            self.invalidate()

        for mode, name, ent_id in tree_decode(data):
            if self.additional_files and (child_additional_files := self.additional_files.files.pop(name, None)):
                if mode == GIT_MODE_TREE:
                    child = GitEntry(self.repo, self, child_additional_files, name, self.name + name + b"/", GIT_MODE_FILE, hexlify(ent_id))
                    yield from child.iter()
                    yield child
                else:
                    old_child = GitEntry(self.repo, self, child_additional_files, name, self.name + name, GIT_MODE_FILE, hexlify(ent_id))
                    item_it = self.repo.cat(old_child.ref)
                    get_oidx, typ, _ = next(item_it)
                    old_contents = b''.join(item_it)
                    new_contents = child_additional_files(old_contents)
                    new_oid = self.repo.write_data(new_contents)
                    yield GitEntry(self.repo, self, None, name, self.name + name, GIT_MODE_FILE, hexlify(new_oid))
                    continue
            else:
                # This entry (and its children) are unmodified, don't recurse.
                yield GitEntry(self.repo, self, child_additional_files, name, self.name + name, mode, hexlify(ent_id))
                                
        if self.additional_files:
            # These are new files and directories that don't exist in the branch
            for name, contents in self.additional_files.files.items():
                if callable(contents):
                    new_oid = self.repo.write_data(contents(None))
                    yield GitEntry(self.repo, self, None, name, self.name + name, GIT_MODE_FILE, hexlify(new_oid))
                else:
                    child = NewDirectoryEntry(self.repo, name, self.name + name + b'/', GIT_MODE_TREE, contents)
                    yield from child.iter()
                    yield child

@dataclass
class CommitHash:
    """A git commit hash, with both human readable and binary representations."""
    binary: bytes
    hex: bytes

    @staticmethod
    def from_hex(hex: bytes) -> 'CommitHash':
        """Create a CommitHash from a hex string."""
        return CommitHash(unhexlify(hex), hex)

    @staticmethod
    def from_binary(binary: bytes) -> 'CommitHash':
        """Create a CommitHash from a byte sequence."""
        return CommitHash(binary, hexlify(binary))

    @staticmethod
    def from_ref(repo: Repo, ref: bytes) -> 'CommitHash':
        """Create a CommitHash from a ref (branchname, tag, or hex string)."""
        commit = repo.read_ref(ref)
        if commit is not None:
            return CommitHash.from_binary(commit)
        return CommitHash.from_hex(ref)


def root_iter(repo: Repo, ref: bytes, additional_files: DirectoryPatch):
    """Generate every new entry created by from applying the patch to the given ref."""
    item_it = repo.cat(ref)
    get_oidx, typ, _ = next(item_it)
    assert typ == b'commit'
    assert get_oidx == ref
    data = b''.join(item_it)
    print("commit data", data)
    tree = data.split(b'\n', maxsplit=1)[0].split(b' ')[1]
    print("tree", tree)
    child = GitEntry(repo, None, additional_files, b"/", b"/", GIT_MODE_TREE, tree)
    if child.additional_files:
        yield from child.iter()
    yield child

class GitReader:
    """Modeled after bup's IndexReader, this class reads a git tree with a patch applied to it."""

    def __init__(self, repo: Repo, additional_files: DirectoryPatch, branchname: bytes):
        self.additional_files = additional_files
        self.branchname = branchname
        self.repo = repo
        self.hash = CommitHash.from_ref(repo, branchname)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __iter__(self, name=None):
        return root_iter(self.repo, self.hash.hex, self.additional_files)

def save_tree(reader: GitReader, repo: Repo):
    # Maintain a stack of information representing the current location in
    # the tree.

    stack = Stack(split_trees=False)
    stack.push(b'', metadata.Metadata())

    fcount = 0
    lastdir = b''
    for ent in reader:
        (dir_path, file) = os.path.split(ent.name)
        exists = (ent.flags & IX_EXISTS)
        already_saved_oid = ent.sha

        fcount += 1

        dirp = path_components(dir_path)

        # At this point, dirp contains a representation of the archive
        # path that looks like [(archive_dir_name, real_fs_path), ...].
        # So given "bup save ... --strip /foo/bar /foo/bar/baz", dirp
        # might look like this at some point:
        #   [('', '/foo/bar'), ('baz', '/foo/bar/baz'), ...].

        # This dual representation supports stripping/grafting, where the
        # archive path may not have a direct correspondence with the
        # filesystem.  The root directory is represented by an initial
        # component named '', and any component that doesn't have a
        # corresponding filesystem directory (due to grafting, for
        # example) will have a real_fs_path of None, i.e. [('', None),
        # ...].

        # If switching to a new sub-tree, finish the current sub-tree.
        while stack.path() > [x[0] for x in dirp]:
            _ = pop(stack, repo)

        # If switching to a new sub-tree, start a new sub-tree.
        for path_component in dirp[len(stack):]:
            dir_name, fs_path = path_component
            # Not indexed, so just grab the FS metadata or use empty metadata.
            stack.push(dir_name, metadata.Metadata())

        if not file:
            if len(stack) == 1:
                continue # We're at the top level -- keep the current root dir
            # Since there's no filename, this is a subdir -- finish it.
            # If the oid isn't already in the bloom filter, we're failing to override.
            # Either walk the full tree to get it in the bloom filter, or make sure we fetch the right id
            oldtree = already_saved_oid # may be False
            newtree = pop(stack, repo, override_tree = oldtree)
            if not oldtree:
                ent.validate(GIT_MODE_TREE, newtree)
            continue

        # it's not a directory
        if already_saved_oid:
            stack.append_to_current(file, ent.mode, ent.gitmode, ent.sha, metadata.Metadata())
        else:
            id = None
            if stat.S_ISDIR(ent.mode):
                assert(0)  # handled above
            elif stat.S_ISLNK(ent.mode):
                mode, id = (GIT_MODE_SYMLINK, repo.write_symlink(metadata.Metadata().symlink_target))
            else:
                # Everything else should be fully described by its
                # metadata, so just record an empty blob, so the paths
                # in the tree and .bupm will match up.
                (mode, id) = (GIT_MODE_FILE, repo.write_data(b''))

            if id:
                ent.validate(mode, id)
                ent.repack()
                stack.append_to_current(file, ent.mode, ent.gitmode, id, metadata.Metadata())

    # pop all parts
    while len(stack) > 0:
        tree = pop(stack, repo)

    return tree

@dataclass
class CommitMetadata:
    userfullname = b'Anonymous'
    username = b'anon'
    hostname = b'localhost'
    commit_msg = b'commit'

    def userline(self) -> bytes:
        return b'%s <%s@%s>' % (self.userfullname, self.username, self.hostname)

def apply_patch(repo: Repo, read_ref: bytes, write_branch: bytes, patch: DirectoryPatch, commit_metadata: CommitMetadata):
    """Produces a new commit with the given patch applied to the given ref."""
    make_commit = True
    now = time.time()
    parent = CommitHash.from_ref(repo, read_ref)
    with GitReader(repo, patch, read_ref) as reader:
        tree = save_tree(reader, repo)
    if make_commit or write_branch:
        userline = commit_metadata.userline()
        commit = repo.write_commit(tree, parent.binary, userline, now, None,
                            userline, now, None, commit_metadata.commit_msg)
        if make_commit:
            log("commit:", hexlify(commit))
            log(b'\n')

    if write_branch:
        repo.update_ref(b'refs/heads/%s' % write_branch, commit, parent.binary)