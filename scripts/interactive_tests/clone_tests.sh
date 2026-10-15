#!/bin/bash

# Usage: clone_tests.sh
#
# Runs the clone pipeline end-to-end against the public annex-test database on DoltHub.
# Requires network access to dolthub.com.

set -e

cd "$(dirname "$0")"
source ../assert.sh

run_authentication_test() (
    clone_url=$1
    clone_remote=$2

    log_header "Testing clone from remote database"

    temp_dir=$(mktemp -d)
    cd $temp_dir

    # Clone the remote database interactively, then exit
    dolt-annex <<EOF
Clone Database
${clone_url}
Local Only
Firefox
Exit
EOF

    # The clone should be a Dolt repository whose origin is the remote database
    assert_true "$(test -d dolt/.dolt && echo true || echo false)" "clone did not create a Dolt repository"

    remote_output=$(cd dolt && dolt remote -v)
    assert_contain "$remote_output" "$clone_remote" "clone's origin does not point at the remote database"

    # The cloned tables should be present and readable locally
    tables_output=$(cd dolt && dolt sql -r csv -q "show tables")
    assert_contain "$tables_output" "submissions" "cloned database is missing the submissions table"
    assert_contain "$tables_output" "metadata" "cloned database is missing the metadata table"

    log_success "Passed: clone from remote database"
)

run_authentication_test \
    "https://www.dolthub.com/repositories/featherbottom/annex-test" \
    "https://doltremoteapi.dolthub.com/featherbottom/annex-test"

run_authentication_test \
    "featherbottom/annex-test" \
    "https://doltremoteapi.dolthub.com/featherbottom/annex-test"

log_success "All tests passed!"
