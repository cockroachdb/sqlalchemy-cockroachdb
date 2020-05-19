# CockroachDB adapter for SQLAlchemy

## Prerequisites

You must install either:

* [psycopg2](https://pypi.org/project/psycopg2/), which has some
  [prerequisites](https://www.psycopg.org/docs/install.html#prerequisites) of
  its own.

* [psycopg2-binary](https://pypi.org/project/psycopg2-binary/)

The binary package is a practical choice for development and testing but in
production it is advised to use the package built from sources.

## Install and usage

Use `pip` to install the latest version.

`pip install sqlalchemy-cockroachdb`

Use a `cockroachdb` connection string when creating the `Engine`. For example,
to connect to an insecure, local CockroachDB cluster:

```
from sqlalchemy import create_engine
engine = create_engine('cockroachdb://root@localhost:26257/defaultdb?sslmode=disable')
```

## Changelog

See [CHANGES.md](https://github.com/cockroachdb/sqlalchemy-cockroachdb/blob/master/CHANGES.md)
