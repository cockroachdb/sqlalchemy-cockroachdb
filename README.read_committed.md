## READ COMMITTED transaction isolation

CockroachDB v23.2.0 added support for READ COMMITTED transaction isolation as
a "preview feature", meaning that we must opt-in to activate it by sending
the statement

```
SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true;
```

Unfortunately, SQLAlchemy's "autobegin"
functionality prevents us from using an `@event.listens_for(Engine, "connect")`
function as that will throw

> sqlalchemy.exc.InternalError: (psycopg2.InternalError) SET CLUSTER SETTING cannot be used inside a multi-statement transaction

Instead, we need to define a custom `connect=` function that we can pass to 
`create_engine()`:

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