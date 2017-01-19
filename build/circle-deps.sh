#!/bin/bash

set -eux -o pipefail

pip install -r dev-requirements.txt

COCKROACH_VERSION=beta-20170112
COCKROACH_PLATFORM=linux-amd64
COCKROACH_NAME=cockroach-${COCKROACH_VERSION}.${COCKROACH_PLATFORM}
DOWNLOAD_DIR=~/cockroach-download

if [ ! -x "${DOWNLOAD_DIR}/${COCKROACH_NAME}/cockroach" ]; then
  mkdir -p "${DOWNLOAD_DIR}"
  curl "https://binaries.cockroachdb.com/${COCKROACH_NAME}.tgz" | tar xzf - -C "${DOWNLOAD_DIR}"
fi
mkdir -p bin
ln -sf "${DOWNLOAD_DIR}/${COCKROACH_NAME}/cockroach" bin
