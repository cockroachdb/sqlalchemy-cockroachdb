#!/bin/bash

set -eux -o pipefail

bin/cockroach start --background
bin/cockroach sql -e 'CREATE DATABASE IF NOT EXISTS test_sqlalchemy'

make test
make check
