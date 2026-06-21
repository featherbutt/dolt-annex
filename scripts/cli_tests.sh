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

output=$(dolt-annex gallery-dl --capture-output https://www.furaffinity.net/view/63142315/)
assert_eq $(echo $output | jq '.submission_files_processed') 1
assert_eq $(echo $output | jq '.submission_metadata_files_processed') 0
assert_eq $(echo $output | jq '.post_metadata_files_processed') 1

expected_file_key="SHA256E-s3204233--28c9485eec3f2e33fa7c0f3c7a5ae62f94e939f3a494e4c5e7dfd16d8c8776c7.png"
output=$(dolt-annex dataset read-table --dataset gallery-dl --table-name submissions)

assert_contain "$output" '"source": "furaffinity.net"'
assert_contain "$output" '"id": 63142315'
assert_contain "$output" "\"submission_file_key\": \"$expected_file_key\""

dolt-annex dataset read-table \
  --dataset gallery-dl \
  --table-name submissions \
  --columns submission_file_key \
  | jq -r '.submission_file_key' | dolt-annex filestore export-file > downloaded_image.png

actual_hash=$(sha256sum downloaded_image.png | awk '{print $1}')
assert_eq "$actual_hash" "28c9485eec3f2e33fa7c0f3c7a5ae62f94e939f3a494e4c5e7dfd16d8c8776c7"

output=$(dolt-annex dataset diff \
  --dataset gallery-dl \
  --table submissions \
  --from __local__ \
  --to remote \
  | cut -d , -f 2 \
  | dolt-annex filestore whereis)
assert_contain "$output" '"name": "__local__"'

# Test filestore make_alias command
md5_file_key="MD5e-s3204233--8bdf45da6e71714bcfafb2c15914cc71.png"

dolt-annex dataset read-table \
  --dataset gallery-dl \
  --table-name submissions \
  --columns submission_file_key \
  | jq -r '.submission_file_key' | dolt-annex filestore make-alias --key-type MD5e

echo $temp_dir
dolt-annex filestore export-file $md5_file_key > downloaded_image.png

md5_hash=$(sha256sum downloaded_image.png | awk '{print $1}')
assert_eq "$md5_hash" "8bdf45da6e71714bcfafb2c15914cc71"


log_success "All tests passed!"