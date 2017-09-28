#!/bin/bash

set -eux -o pipefail

pip install -r dev-requirements.txt

for COCKROACH_VERSION in v1.0.6 v1.1-beta.20170921; do
  COCKROACH_PLATFORM=linux-amd64
  COCKROACH_NAME=cockroach-${COCKROACH_VERSION}.${COCKROACH_PLATFORM}
  DOWNLOAD_DIR=~/cockroach-download

  if [ ! -x "${DOWNLOAD_DIR}/${COCKROACH_NAME}/cockroach" ]; then
    mkdir -p "${DOWNLOAD_DIR}"
    curl "https://binaries.cockroachdb.com/${COCKROACH_NAME}.tgz" | tar xzf - -C "${DOWNLOAD_DIR}"
  fi
  mkdir -p bin
  ln -sf "${DOWNLOAD_DIR}/${COCKROACH_NAME}/cockroach" "bin/cockroach-${COCKROACH_VERSION:0:4}"
done
