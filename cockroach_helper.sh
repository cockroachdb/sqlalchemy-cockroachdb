#!/bin/bash
COCKROACHDB=cockroach-v23.1.13.linux-amd64
CROACHDB=~/.cache/$COCKROACHDB/cockroach

quit_cockroachdb() {
    OLDPIDNS=$(ps -o pidns -C cockroach | awk 'NR==2 {print $0}')
    if [ -n "$OLDPIDNS" ]; then
        pkill --ns $$ $OLDPIDNS
    fi
    return 0
}

[ -n "$HOST" ] || HOST=localhost
mkdir -p $(dirname $CROACHDB)
[[ -f "$CROACHDB" ]] || wget -qO- https://binaries.cockroachdb.com/$COCKROACHDB.tgz | tar xvz --directory ~/.cache
if [ $1 == "start" ]; then
    quit_cockroachdb
    $CROACHDB start-single-node --background --insecure --store=type=mem,size=10% --log-dir /tmp/ --listen-addr=$HOST:26257 --http-addr=$HOST:26301
    #$CROACHDB sql --host=$HOST:26257 --insecure -e "set sql_safe_updates=false; drop database if exists apibuilder; create database if not exists apibuilder; create user if not exists apibuilder; grant all on database apibuilder to apibuilder;"
else
    quit_cockroachdb
fi
