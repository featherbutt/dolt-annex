#!/bin/bash

# Usage: interactive_tests.sh [no-authentication | unencrypted-key]
#
# Each argument selects how the test SFTP server authenticates clients. With no
# argument, every configuration is tested in turn.

set -e

cd "$(dirname "$0")"
source ./assert.sh

# Runs the full end-to-end test for one authentication configuration. The body is
# a subshell so its `cd`s don't leak into the next configuration.
run_test() (
    local auth_mode=$1

    sftp_url="sftp://127.0.0.1:2222/"
    client_key_answer=""
    password=""
    password_initial_prompt_answer=$'n\n'
    password_subsequent_prompt_answer=""
    case "$auth_mode" in
        no-authentication)
            server_auth_args=(--no-authentication)
            ;;
        password-in-url)
            server_auth_args=(--password password)
            sftp_url="sftp://user:password@127.0.0.1:2222/"
            password_initial_prompt_answer=""
            ;;
        password-prompt)
            server_auth_args=(--password password)
            password_initial_prompt_answer=$'y\npassword\npassword\n'
            password_subsequent_prompt_answer=$'password\n'
            ;;
        unencrypted-key)
            server_auth_args=(--authorized-keys ../id_ed25519.pub)
            client_key_answer="../id_ed25519"
            ;;
        encrypted-key)
            server_auth_args=(--authorized-keys ../id_ed25519.pub --server-key-passphrase password)
            client_key_answer="../id_ed25519"
            password="password"
            password_initial_prompt_answer=$'y\npassword\npassword\n'
            password_subsequent_prompt_answer=$'password\n'
            ;;
        *)
            echo "Unknown authentication configuration: $auth_mode"
            return 1
            ;;
    esac

    log_header "Testing authentication configuration: $auth_mode"

    temp_dir=$(mktemp -d)
    cd $temp_dir

    ssh-keygen -t ed25519 -f ./id_ed25519 -N "$password" -q

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

    dolt-annex server "${server_auth_args[@]}" --host 127.0.0.1 --port 2222 --server-key ../id_ed25519 &
    server_pid=$!
    cd dolt
    dolt sql-server &
    sql_pid=$!

    sleep 1

    cd ../../downstream

    dolt-annex <<EOF
Connect to Remote Database
mysql://root@127.0.0.1/dolt
n
Remote Only
$sftp_url
${client_key_answer}
${password_initial_prompt_answer}Firefox
Exit
EOF

    dolt-annex <<EOF
${password_subsequent_prompt_answer}Exit
EOF

    dolt-annex <<EOF
${password_subsequent_prompt_answer}Gallery-dl Download
https://e621.net/posts/6780547

Exit
EOF

    kill $server_pid $sql_pid
    wait $server_pid $sql_pid 2>/dev/null || true

    log_success "Passed: $auth_mode"
)

if [ $# -eq 0 ]; then
    set -- no-authentication password-in-url password-prompt unencrypted-key encrypted-key
fi

for auth_mode in "$@"; do
    run_test "$auth_mode"
done

log_success "All tests passed!"
