#!/bin/bash

set -euo pipefail

ENV=$(mktemp -d)

if [ ! -f "${1}.in" ]; then
  echo "input file $1.in not found"
  exit 1
fi

virtualenv -p python3.5 "${ENV}"
"${ENV}/bin/pip" install -r "${1}.in"
"${ENV}/bin/pip" freeze > "$1"
rm -rf "${ENV}"
