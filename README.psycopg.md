## psycopg support

Support for psycopg version 3 (sometimes referred to as "psycopg3") requires the 
following *minimum* CockroachDB versions: 22.2.6 or 23.1.0

### sync operation

The connection URL is of the form:
```
cockroachdb+psycopg://root@localhost:26257/defaultdb
```

To create the engine

```
from sqlalchemy import create_engine
engine = create_engine('cockroachdb+psycopg://root@localhost:26257/defaultdb')
```

### async operation (⚠️ experimental 🏗)

The "classic" approach

```
from sqlalchemy.ext.asyncio import create_async_engine
engine = create_async_engine('cockroachdb+psycopg://root@localhost:26257/defaultdb')
```

does work, but it does not take advantage of the CockroachDB-specific connection code
in psycopg; we just get a plain `psycopg.AsyncConnection`. After 
`cnxn = await engine.raw_connection()` we get

```
(Pdb) cnxn.driver_connection
<psycopg.AsyncConnection … >
```

The alternative approach is to use the following

```python
import psycopg.crdb
from sqlalchemy.ext.asyncio import create_async_engine

async def get_async_crdb_connection():
    return await psycopg.crdb.AsyncCrdbConnection.connect(
        "host=localhost port=26257 user=root dbname=defaultdb"
    )

async def async_main():
    engine = create_async_engine(
        "cockroachdb+psycopg://", 
        async_creator=get_async_crdb_connection,
    )

```

which gives us an `AsyncCrdbConnection`

```
(Pdb) cnxn.driver_connection
<psycopg.crdb.AsyncCrdbConnection … >
```
