#!/bin/bash

set -eux -o pipefail

source env/bin/activate

# TODO(bdarnell): versions 2.0 and 1.1 currently fail because of the
# LIKE ESCAPE operator, and we don't have a clean way to disable that
# part of the test suite.

for version in v2.1; do
  rm -rf cockroach-data
  "cockroach-$version" start --background --insecure --pid-file=cockroach.pid
  "cockroach-$version" sql --insecure -e 'CREATE DATABASE IF NOT EXISTS test_sqlalchemy'

  make test
  kill -9 $(cat cockroach.pid)
done

make lint
