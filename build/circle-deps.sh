#!/bin/bash

set -eux -o pipefail

if [ ! -x env/bin/python ]; then
    python3 -m venv env
fi
source env/bin/activate

pip install -r dev-requirements.txt

case $OSTYPE in
    darwin*) COCKROACH_PLATFORM=darwin-10.9-amd64;;
    linux*) COCKROACH_PLATFORM=linux-amd64;;
    *) echo "Unsupported platform"; exit 1;;
esac

for COCKROACH_VERSION in v2.1.3; do
  COCKROACH_NAME=cockroach-${COCKROACH_VERSION}.${COCKROACH_PLATFORM}
  DOWNLOAD_DIR=~/cockroach-download

  if [ ! -x "${DOWNLOAD_DIR}/${COCKROACH_NAME}/cockroach" ]; then
    mkdir -p "${DOWNLOAD_DIR}"
    curl "https://binaries.cockroachdb.com/${COCKROACH_NAME}.tgz" | tar xzf - -C "${DOWNLOAD_DIR}"
  fi
  ln -sf "${DOWNLOAD_DIR}/${COCKROACH_NAME}/cockroach" "env/bin/cockroach-${COCKROACH_VERSION:0:4}"
done
