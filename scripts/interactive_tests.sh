#!/bin/bash

# This contains some simple end-to-end tests to verify that the docker container
# built correctly.

set -e

cd "$(dirname "$0")"
source ./assert.sh

temp_dir=$(mktemp -d)
cd $temp_dir

ssh-keygen -t ed25519 -f ./id_ed25519_unencrypted -N "" -q

mkdir upstream
mkdir downstream
cd upstream

# Initialize the dolt-annex repository interactively with a local database and filestore
dolt-annex <<EOF
Create New Local Database

Local Only
Firefox
Exit
EOF

# Launch the dolt-annex interactive prompt and immediately exit, asserting the setup has already been completed successfully
dolt-annex <<EOF
Exit
EOF

dolt-annex server --authorized-keys ../id_ed25519_unencrypted.pub --host 127.0.0.1 --port 2222 --server-key ../id_ed25519_unencrypted &
cd dolt
dolt sql-server &

sleep 1

cd ../../downstream

dolt-annex <<EOF
Connect to Remote Database
mysql://root@127.0.0.1/dolt
n
Remote Only
sftp://127.0.0.1:2222/
../id_ed25519_unencrypted
n
Firefox
Exit
EOF

dolt-annex <<EOF
Exit
EOF

dolt-annex <<EOF
Gallery-dl Download
https://e621.net/posts/6780547

Exit
EOF

kill %1
kill %2
log_success "All tests passed!"