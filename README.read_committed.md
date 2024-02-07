## READ COMMITTED transaction isolation

CockroachDB v23.2.0 added support for READ COMMITTED transaction isolation as
a "preview feature", meaning that we must opt-in to activate it by sending
the statement

```
SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true;
```

This statement changes a persisted setting in the CockroachDB cluster. It is meant
to be executed one time by a database operator/administrator.

For testing purposes, this adapter offers a custom `connect=` function that we
can pass to  `create_engine()`, which will configure this setting:

```python
import psycopg2
from sqlalchemy import create_engine

def connect_for_read_committed():
    cnx = psycopg2.connect("host=localhost port=26257 user=root dbname=defaultdb")
    cnx.autocommit = True
    crs = cnx.cursor()
    crs.execute("SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true;")
    cnx.autocommit = False
    return cnx

engine = create_engine(
    "cockroachdb+psycopg2://",
    creator=connect_for_read_committed,
    isolation_level="READ COMMITTED",
)

with engine.begin() as conn:
    conn.exec_driver_sql("UPDATE tbl SET txt = 'SQLAlchemy' WHERE id = 1")
```
