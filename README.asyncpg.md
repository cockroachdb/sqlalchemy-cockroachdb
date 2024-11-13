## asyncpg support

The connection URL is of the form:

    cockroachdb+asyncpg://root@localhost:26257/defaultdb

There is a customized version of the FastAPI SQL database tutorial for
`cockroachdb+asyncpg` available at

https://github.com/gordthompson/fastapi-tutorial-cockroachdb-async

### Default transaction isolation level

Applications using asyncpg that were developed prior to CockroachDB's inclusion of
READ COMMITTED transaction isolation may operate on the assumption that the default
isolation level will be SERIALIZABLE. For example, 

```python
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine


async def async_main():
    engine = create_async_engine(
        "cockroachdb+asyncpg://root@localhost:26257/defaultdb",
    )
    async with engine.begin() as conn:
        result = await conn.exec_driver_sql("select version()")
        print(result.scalar().split("(")[0])  # CockroachDB CCL v23.2.4
        
        result = await conn.exec_driver_sql("show transaction isolation level")
        print(result.scalar())  # serializable


asyncio.run(async_main())
```

With current versions of CockroachDB, the default transaction isolation level
**for asyncpg only** is now READ COMMITTED

```python
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine


async def async_main():
    engine = create_async_engine(
        "cockroachdb+asyncpg://root@localhost:26257/defaultdb",
    )
    async with engine.begin() as conn:
        result = await conn.exec_driver_sql("select version()")
        print(result.scalar().split("(")[0])  # CockroachDB CCL v24.3.0
        
        result = await conn.exec_driver_sql("show transaction isolation level")
        print(result.scalar())  # read committed


asyncio.run(async_main())
```

Applications that rely on the original behavior will have to add `isolation_level="SERIALIZABLE"`
to their `create_async_engine()` call

```python
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine


async def async_main():
    engine = create_async_engine(
        "cockroachdb+asyncpg://root@localhost:26257/defaultdb",
        isolation_level="SERIALIZABLE",
    )
    async with engine.begin() as conn:
        result = await conn.exec_driver_sql("select version()")
        print(result.scalar().split("(")[0])  # CockroachDB CCL v24.3.0

        result = await conn.exec_driver_sql("show transaction isolation level")
        print(result.scalar())  # serializable


asyncio.run(async_main())
```

### Testing

Assuming that you have an entry in test.cfg that looks something like

    [db]
    asyncpg=cockroachdb+asyncpg://root@localhost:26257/defaultdb

you can run the tests with asyncpg using a command like

    pytest --db=asyncpg

If you want to run all the tests *except* the Alembic tests then invoke pytest
using a command like

    pytest --db=asyncpg --ignore-glob='*test_suite_alembic.py'
