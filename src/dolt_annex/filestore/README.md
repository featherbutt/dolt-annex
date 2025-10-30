# Filestore

A Filestore is a mechanism by which a repo stores annexed files on disk. There are multiple different filestore types, including the "mount" type, which combines multiple child filestores and exposes them via a single interface.

The names and configuration options are inspired by [IPFS's datastore configuration options.](https://github.com/ipfs/kubo/blob/master/docs/datastores.md)

When connecting to a remote, a client doesn't need to know how that remote manages its filestore. The client communicates with the remote over a standard interface (such as SFTP, RPC, or FUSE), and the remote translates those operations for its underlying filestore.

