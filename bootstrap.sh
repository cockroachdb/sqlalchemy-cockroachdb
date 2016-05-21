#!/bin/bash

ENV_BASE=~/envs
ENV=$ENV_BASE/cockroach-python

if [[ ! -d $ENV ]]; then
    mkdir -p $ENV
    virtualenv $ENV
fi

$ENV/bin/pip install -r dev-requirements.txt
