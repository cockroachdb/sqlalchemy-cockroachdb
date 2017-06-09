#!/bin/bash

set -eux -o pipefail

bin/cockroach start --background --insecure
bin/cockroach sql --insecure -e 'CREATE DATABASE IF NOT EXISTS test_sqlalchemy'

make test
make lint
