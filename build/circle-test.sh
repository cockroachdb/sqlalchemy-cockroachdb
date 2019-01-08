#!/bin/bash

set -eux -o pipefail

source env/bin/activate

for executable in env/bin/cockroach-*; do
  rm -rf cockroach-data
  ${executable} start --background --insecure --pid-file=cockroach.pid
  ${executable} sql --insecure -e 'CREATE DATABASE IF NOT EXISTS test_sqlalchemy'

  make test
  kill -9 $(cat cockroach.pid)
done

make lint
