from binascii import hexlify, unhexlify
from dataclasses import dataclass
import os
import stat
import time
from typing import Literal, Optional

from bup import metadata
from bup.cmd.save import save_tree
from bup.hashsplit import GIT_MODE_FILE, GIT_MODE_SYMLINK, GIT_MODE_TREE
from bup.helpers import path_components
from bup.repo import LocalRepo
from bup.repo.base import RepoProtocol as Repo
from bup.git import tree_decode
from bup.index import IX_HASHVALID, IX_EXISTS, IX_SHAMISSING, Entry as BupEntry
from bup.options import Options

from bup.tree import Stack, _write_tree
from logger import logger

from .patch import DirectoryPatch, new_file

log = logger.info

GIT_MODE_FILE = 0o100644
GIT_MODE_TREE = 0o40000
GIT_MODE_SYMLINK = 0o120000

def pop(stack, repo, override_tree=None):
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

    def invalidate(self):
        self.flags &= ~IX_HASHVALID
        self.sha = None
        if self.parent:
            self.parent.invalidate()

    def repack(self):
        pass

    def iter(self, name=None, wantrecurse=None):
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
        print("GitEntry.ref:", ref)
        print("GitEntry.basename:", self.basename)
        print("GitEntry.name:", self.name)

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
            print("ent_id", hexlify(ent_id))
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

def resolve_branch_or_ref(repo, branchname):
    ref = repo.read_ref(branchname)
    if ref is not None:
        return ref
    return unhexlify(branchname)

def root_iter(repo: Repo, ref: bytes, additional_files: DirectoryPatch):
    item_it = repo.cat(ref)
    get_oidx, typ, _ = next(item_it)
    assert typ == b'commit'
    assert get_oidx == ref
    data = b''.join(item_it)
    print("commit data", data)
    tree = data.split(b'\n')[0].split(b' ')[1]
    print("tree", tree)
    child = GitEntry(repo, None, additional_files, b"/", b"/", GIT_MODE_TREE, tree)
    if child.additional_files:
        yield from child.iter()
    yield child

        

class GitReader:
    def __init__(self, repo: Repo, additional_files: DirectoryPatch, branchname: bytes):
        self.additional_files = additional_files
        self.branchname = branchname
        self.repo = repo
        oid = resolve_branch_or_ref(repo, branchname)
        print(f"repo ref {branchname}", hexlify(oid))

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def iter(self, name=None, wantrecurse=None):
        dname = name
        if dname and not dname.endswith(b'/'):
            dname += b'/'
        ref_bytes = resolve_branch_or_ref(self.repo, self.branchname)
        ref = hexlify(ref_bytes)
        # TODO: Don't treat the commit like a tree.
        root = GitEntry(self.repo, None, self.additional_files, b'/', b'/', GIT_MODE_TREE, ref)
        yield from root_iter(self.repo, ref, self.additional_files)


    def find(self, name):
        return next((e for e in self.iter(name, wantrecurse=lambda x : True)
                     if e.name == name),
                    None)

    def filter(self, wantrecurse=None):
        for e in self.iter(wantrecurse=wantrecurse):
            yield (e.name, e)

def save_tree(opt, reader, hlink_db, msr, repo, split_trees):
    # Maintain a stack of information representing the current location in
    # the tree.

    stack = Stack(split_trees=split_trees)
    stack.push(b'', metadata.Metadata())

    def already_saved(ent) -> Literal[False] | bytes:
        return ent.is_valid() and repo.exists(ent.sha) and ent.sha

    fcount = 0
    lastdir = b''
    for transname, ent in reader.filter():
        (dir, file) = os.path.split(ent.name)
        exists = (ent.flags & IX_EXISTS)
        already_saved_oid = ent.sha #already_saved(ent)

        fcount += 1

        dirp = path_components(dir)

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

def make_bup_options() -> Options:
    opt = Options('bup save')
    opt.commit = True
    opt.verbose = True
    opt.tree = True
    opt.commit = True
    opt.smaller = False
    opt.strip = False
    opt.strip_path = False
    opt.grafts = False
    opt.date = time.time()
    opt.name = b"git-annex"
    return opt

@dataclass
class CommitMetadata:
    userfullname = b'Anonymous'
    username = b'anon'
    hostname = b'localhost'
    commit_msg = b'commit'

    def userline(self) -> bytes:
        return b'%s <%s@%s>' % (self.userfullname, self.username, self.hostname)

def main(argv):
    repo = LocalRepo(b'git-annex')
    with repo:
        refname = b'refs/heads/git-annex'
        parent = resolve_branch_or_ref(repo, refname)
        opt = make_bup_options()
        userfullname = b'Anonymous'
        username = b'anon'
        hostname = b'localhost'
        commit_msg = b'commit'
        additional_files = DirectoryPatch(
            dirs = { b"ff7" : DirectoryPatch({}, {b"ftest.txt": new_file(b"test3")}), b"test": DirectoryPatch({}, {b"test.txt": new_file(b"test1")})},
            files = {b"test.txt": new_file(b"test2")})
        with GitReader(repo, additional_files, refname) as reader:
            tree = save_tree(opt, reader, None, None, repo, False)
        if opt.tree:
            log("saved tree", hexlify(tree))
            log(b'\n')
        if opt.commit or opt.name:
            userline = (b'%s <%s@%s>' % (userfullname, username, hostname))
            commit = repo.write_commit(tree, parent, userline, opt.date, None,
                             userline, opt.date, None, commit_msg)
            if opt.commit:
                log("commit:", hexlify(commit))
                log(b'\n')

        if opt.name:
            repo.update_ref(b'refs/heads/%s' % opt.name, commit, parent)

def apply_patch(repo: Repo, ref: bytes, patch: DirectoryPatch, commit_metadata: CommitMetadata):
    opts = make_bup_options()
    parent = resolve_branch_or_ref(repo, ref)
    with GitReader(repo, patch, ref) as reader:
        tree = save_tree(opts, reader, None, None, repo, False)
    if opts.tree:
        log("saved tree", hexlify(tree))
        log(b'\n')
    if opts.commit or opts.name:
        userline = commit_metadata.userline()
        commit = repo.write_commit(tree, parent, userline, opts.date, None,
                            userline, opts.date, None, commit_metadata.commit_msg)
        if opts.commit:
            log("commit:", hexlify(commit))
            log(b'\n')

    if opts.name:
        repo.update_ref(b'refs/heads/%s' % opts.name, commit, parent)