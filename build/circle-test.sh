#!/bin/bash

set -eux -o pipefail

for version in v1.0 v1.1; do
  rm -rf cockroach-data
  "bin/cockroach-$version" start --background --insecure --pid-file=cockroach.pid
  "bin/cockroach-$version" sql --insecure -e 'CREATE DATABASE IF NOT EXISTS test_sqlalchemy'

  make test
  kill -9 $(cat cockroach.pid)
done

make lint
