#!/bin/bash

# This contains some simple end-to-end tests to verify that the docker container
# built correctly.

set -e

cd "$(dirname "$0")"
source ./assert.sh

temp_dir=$(mktemp -d)
cd $temp_dir

# TODO: Also accept more readable string input for interactive tests

# Initialize the dolt-annex repository interactively with a local database and filestore
dolt-annex <<EOF
3

1
3
EOF

# Launch the dolt-annex interactive prompt and immediately exit, asserting the setup has already been completed successfully
dolt-annex <<EOF
3
EOF

log_success "All tests passed!"