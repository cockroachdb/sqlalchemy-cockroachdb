# CockroachDB dialect for SQLAlchemy

## Prerequisites

A database driver (DBAPI layer) is required to work with this dialect.

### psycopg2

For psycopg2 support you must install either:

* [psycopg2](https://pypi.org/project/psycopg2/), which has some
  [prerequisites](https://www.psycopg.org/docs/install.html#prerequisites) of
  its own, or

* [psycopg2-binary](https://pypi.org/project/psycopg2-binary/)

(The binary package is a practical choice for development and testing but in
production it is advised to use the package built from sources.)

### asyncpg

For asyncpg support you must install

* [asyncpg](https://pypi.org/project/asyncpg/)

For more details on working with asyncpg, see 
[README.asyncpg.md](README.asyncpg.md)

### psycopg

For psycopg version 3 support (⚠️ experimental 🏗), you'll need to install

* [psycopg](https://pypi.org/project/psycopg/)

As with psycopg2, psycopg can be installed as binary for development and testing purposes.
(Installing as binary avoids the need for the libpq-dev package to be installed first.)

`pip install psycopg[binary]`

For more details on working with psycopg, see 
[README.psycopg.md](README.psycopg.md)
 
## Install and usage

Use `pip` to install the latest release of this dialect.

```
pip install sqlalchemy-cockroachdb
```

NOTE: This version of the dialect requires SQLAlchemy 2.0 or later. To work with
earlier versions of SQLAlchemy you'll need to install an earlier version of this
dialect.

```
pip install sqlalchemy-cockroachdb<2.0.0
```

Use a `cockroachdb` connection string when creating the `Engine`. For example,
to connect to an insecure, local CockroachDB cluster using psycopg2:

```
from sqlalchemy import create_engine
engine = create_engine('cockroachdb://root@localhost:26257/defaultdb?sslmode=disable')
```

or

```
from sqlalchemy import create_engine
engine = create_engine('cockroachdb+psycopg2://root@localhost:26257/defaultdb?sslmode=disable')
```

To connect using asyncpg:

```
from sqlalchemy.ext.asyncio import create_async_engine
engine = create_async_engine('cockroachdb+asyncpg://root@localhost:26257/defaultdb')
```

To connect using psycopg for sync operation:

```
from sqlalchemy import create_engine
engine = create_engine('cockroachdb+psycopg://root@localhost:26257/defaultdb')
```

To connect using psycopg for async operation (⚠️ experimental 🏗), see
[README.psycopg.md](README.psycopg.md)


## Changelog

See [CHANGES.md](CHANGES.md)
