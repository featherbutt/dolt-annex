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

Python 3.10+ is required.

## Installation

dolt-annex is pure python. To install, simply clone and run `pip install ./src`.

## Running

Before running dolt-annex for the first time, read through [CONCEPTS.md](CONCEPTS.md). 

The curent set of useful subcommands are:

- `init` - creates a basic environment with sensible defaults in the current directory. This isn't necessary if you're going to set up your environment yourself, but looking at its implementation (in `commands/init.py`) is helpful for seeing what needs to be done to configure your repo.
- `import` - import a local directory into to your annex.
- `push` - upload files from your annex to a remote.
- `pull` - download files from a remote to your annex.
- `gallery-dl` - uses [gallery-dl](https://github.com/mikf/gallery-dl) to download files from a site supported by gallery-dl, and imports them into your annex.
- `server` - create a sandboxed SFTP server, allowing dolt-annex to act as a remote.

## Docker quickstart

TODO: Re-verify these instructions for a fresh install. For example, config.json should have `spawn_dolt_server = false` probably.

```
$ git clone https://github.com/featherbutt/dolt-annex.git
$ cd dolt-annex
$ docker build -t dolt-annex-image
$ mkdir -p /path/to/where/you/want/dolt-annex
$ CONTAINER_ID="$(docker run -d -v /path/to/where/you/want/dolt-annex:/repo dolt-annex-image)"
$ docker exec -d "${CONTAINER_ID}" dolt-annex gallery-dl https://e621.net/posts/4490888
$ docker exec -it "${CONTAINER_ID}" bash  # From here you can run other dolt-annex commands
```

To manage future runs, you can even create a systemd unit file, such as:

```
dolt_annex_d.sh:
----------------
#!/bin/bash
CONTAINER_ID="$(docker run -d -v /path/to/where/you/want/dolt-annex:/repo dolt-annex-image)"
docker exec "${CONTAINER_ID}" dolt-annex gallery-dl '[URL to search results page you want gallery-dl to paginate and download]'
docker kill "${CONTAINER_ID}"
```

```
/etc/systemd/system/dolt-annex-d.service
--------------------------------------
[Unit]
Description=dolt-annex and long-running gallery-dl

[Service]
ExecStart=/path/to/dolt_annex_d.sh
User=[me]
Group=[me]

[Install]
WantedBy=multi-user.target
```

```
$ systemctl start dolt-annex-d
$ systemctl enable dolt-annex-d
```

And further docker administration can be done by finding the container id via `docker ps` and bash exec-ing into it.

## `dolt-annex import` 

Format: `dolt-annex import [--move|--copy|--symlink] --importer $IMPORTER --dataset $DATASET" --file-key-type $FILE_KEY_TYPE $DIRECTORY`

Example command: `dolt-annex import --move --importer "DirectoryImporter prefix.com/files/" --dataset mydataset --file-key-type Sha256e ~/Downloads/prefix.com/files`

## `dolt-annex pull` and `dolt-annex push`

## `dolt-annex gallery-dl`

```
$ dolt-annex gallery-dl https://e621.net/posts/4490888
```

Would use gallery-dl to download this link. Gallery-dl can also be used to automatically paginate and download results from search pages of some websites. Dolt-annex's use of gallery-dl can be further configured in `gallery_dl_config.json`.

## `dolt-annex dataset read-table`

```
$ dolt-annex dataset read-table --dataset gallery-dl --table-name submissions
````

Would list all gallery-dl submissions. If the file exists, see `gallery-dl.dataset` for other tables that can be listed.

Append `--where source=archiveofourown.org` to filter to just AO3 submissions.

## `dolt-annex filestore export-file`

```
$ dolt-annex filestore export-file --file-key SHA256E-s16682--174324580ef0a850a2223b74fa305eb92b39ddfbff785c92076c67a2e9b0e412.html
```

To export a specific file. Obtain file-keys from commands like `dolt-annex dataset read-table` above.

