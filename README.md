# dolt-annex: Distributed File Store at Scale

dolt-annex is a tool for replicating large datasets between multiple repositories, without requiring each repository to contain a copy of every file. Instead, dolt-annex maintains tracking information about which repositories contain copies of which files, allowing files to be retrieved on demand.

## How is this different from git-annex?

git-annex has scaling issues that make it unsuitable for extremely large datasets:

- git-annex stores metadata about annexed files but this metadata is not indexed. [There's an experiment to store metadata in a sqlite database](https://git-annex.branchable.com/design/caching_database/), but the database needs to be generated locally.
- git-annex syncing operations (get, push, pull, sync) scale with the number of annexed files in the branches being synced. It's not possible to efficiently identify only the set of the annexed files that need to be transferred. These commands also rely on normal git branches containing symlinks to annexed files as a measure of liveness/reachability.
- These normal git branches also add additional overhead, especially in the case where the dataset doesn't have an obvious file system representation.

Ideally, we want to be able to store all tracking information in a database that is itself decentralized, version-controlled and can be merged during sync operations.

[Dolt](https://www.dolthub.com/) does exactly what we need.

dolt-annex attempts to tackle these scaling issues by replacing the git-annex repo with a Dolt repo. Dolt tracks the known locations of annexed files, and updates this information whenever it syncs with its remotes.

An unrelated scaling issue for git-annex is that the standard git client creates a new file for every git object. By using a running Dolt server, we can more intelligently batch writes to the file system.

## Requirements

dolt-annex depends on [Dolt](https://github.com/dolthub/dolt). Dolt can be installed locally, or dolt-annex can connect to a running Dolt server.

Python 3.13+ is required.

## Installation

dolt-annex is pure python. To install, simply clone and run `pip install ./src`.

## Running

Before running dolt-annex for the first time, read through [CONCEPTS.md](CONCEPTS.md). 

The curent set of useful subcommands are:

- `init` - creates a basic environment with sensible defaults in the current directory. This isn't necessary if you're going to set up your environment yourself, but looking at its implementation (in `commands/init.py`) is helpful for seeing what needs to be done to configure your repo.
- `create` - Define a new repo or dataset schema.
- `push` - upload files from your repo to a remote repo.
- `pull` - download files from a remote repo to your repo.
- `gallery-dl` - uses [gallery-dl](https://github.com/mikf/gallery-dl) to download files from a site supported by gallery-dl, and imports them into your repo.
- `insert-record` - Add a new file to a dataset.
- `server` - create a sandboxed SFTP server, allowing dolt-annex to act as a remote.

There is not currently an easy way to create new datasets or share dataset schemas, and no easy way to add files to a repo other calling `dolt-annex insert-record` for each file, or using `dolt-annex gallery-dl`. It's possible to write scripts that bulk import files by directly accessing both the Dolt database and the underlying filestore. The roadmap is to both add additional CLI tools for data imports, and make dolt-annex usable as a library.

## `dolt-annex pull` and `dolt-annex push`

## `dolt-annex gallery-dl`
