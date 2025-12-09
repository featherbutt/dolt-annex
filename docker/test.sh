#!/bin/bash

# This contains some simple end-to-end tests to verify that the docker container
# built correctly.

set -e

cd "$(dirname "$0")"
source ./assert.sh

temp_dir=$(mktemp -d)
cd $temp_dir

dolt-annex init

output=$(dolt-annex gallery-dl https://www.furaffinity.net/view/63142315/)
assert_eq $(echo $output | jq '.submission_files_processed') 1
assert_eq $(echo $output | jq '.submission_metadata_files_processed') 0
assert_eq $(echo $output | jq '.post_metadata_files_processed') 1

output=$(dolt-annex read-table --dataset gallery-dl --table-name submissions)
assert_eq "$output" "SHA256E-s3204233--28c9485eec3f2e33fa7c0f3c7a5ae62f94e939f3a494e4c5e7dfd16d8c8776c7.png, furaffinity.net, 63142315, 2025-11-28 20:36:17, 1"
