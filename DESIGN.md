dolt_annex is organized into several subpackages:

## dolt_annex.commands

Contains implementations for each subcommand. This subpackage should contain minimal business logic: it should be possible to depend on dolt_annex as a library without ever importing anything from this package.

## dolt_annex.datatypes

Defines common datatypes and code for loading, validation, and serialization.

## dolt_annex.file_keys

Defines the FileKey abstract base class for content-described file keys, along with its implementations.

## dolt_annex.filestore

Defines the FileStore abstract base class for storing and retrieving files, and its implementations.

## dolt_annex.gallery_dl

Allows for invoking `gallery-dl` with a custom configuration, plus postprocessor hooks that import the downloaded files into a filestore.

## dolt_annex.importers

Defines the Importer abstract base class for importing local files into a filestore.

## dolt_annex.server

A sandboxed SFTP server, used for creating remotes with the `server` subcommand.

## dolt_annex.sync

Contains common logic for file sync operations, such as the `push`, `pull` and `sync` subcommands.