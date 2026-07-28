#!/bin/bash

# This contains some simple end-to-end tests to verify that the docker container
# built correctly.

set -e

cd "$(dirname "$0")"
source ./assert.sh

temp_dir=$(mktemp -d)
cd $temp_dir

dolt-annex init

dolt-annex create repo remote '{
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
            "filestore": {"type": "annexfs", "root": "."},
            "key_format": "SHA256E"
        }'

output=$(dolt-annex gallery-dl --capture-output https://e621.net/posts/14)
assert_eq $(echo $output | jq '.submission_files_processed') 1
assert_eq $(echo $output | jq '.submission_metadata_files_processed') 0
assert_eq $(echo $output | jq '.post_metadata_files_processed') 1

expected_file_key="MD5_HSe-3e47080200fbde2d7d2ccf419343ab0a--s96998.jpg"
output=$(dolt-annex dataset read-table --dataset gallery-dl --table-name submissions)

assert_contain "$output" '"source": "e621.net"'
assert_contain "$output" '"id": 14'
assert_contain "$output" "\"submission_file_key\": \"$expected_file_key\""

dolt-annex dataset read-table \
  --dataset gallery-dl \
  --table-name submissions \
  --columns submission_file_key \
  | jq -r '.submission_file_key' | dolt-annex filestore export-file > downloaded_image.png

actual_hash=$(md5sum downloaded_image.png | awk '{print $1}')
assert_eq "$actual_hash" "3e47080200fbde2d7d2ccf419343ab0a"

output=$(dolt-annex dataset diff \
  --dataset gallery-dl \
  --table submissions \
  --from __local__ \
  --to remote \
  | cut -d , -f 2 \
  | dolt-annex filestore whereis)
assert_contain "$output" '"name": "__local__"'

# Test filestore make_alias command
sha1_file_key="SHA1_HSe-8e5544bfa47e6ac5b3c69f69f2f38f2503aa62f4--s96998.jpg"

dolt-annex dataset read-table \
  --dataset gallery-dl \
  --table-name submissions \
  --columns submission_file_key \
  | jq -r '.submission_file_key' | dolt-annex filestore make-alias --key-type SHA1_HSe

echo $temp_dir
dolt-annex filestore export-file $sha1_file_key > downloaded_image.png

sha1_hash=$(sha1sum downloaded_image.png | awk '{print $1}')
echo $sha1_hash
assert_eq "$sha1_hash" "8e5544bfa47e6ac5b3c69f69f2f38f2503aa62f4"


log_success "All tests passed!"