## asyncpg support (experimental)

Preliminary support for asyncpg has been added. The connection URI is of the form::

    cockroachdb+asyncpg://root@localhost:26257/defaultdb

The example code at

https://docs.sqlalchemy.org/en/14/_modules/examples/asyncio/async_orm.html

runs without error and seems to be producing reasonable results.

There is also a customized version of the FastAPI SQL database tutorial for
`cockroachdb+asyncpg` available at

https://github.com/gordthompson/fastapi-tutorial-cockroachdb-async

### Testing

asyncpg support has not yet been added to CI testing. There is at least one outstanding issue
that needs to be resolved before the Alembic test suite can be successfully run on an asyncpg
connection:

https://github.com/cockroachdb/cockroach/issues/71908

So, if you want to run all of the tests *except* the Alembic tests then invoke pytest
using a command like

    pytest --db=asyncpg --ignore-glob='*test_suite_alembic.py'

assuming that you have an entry in test.cfg that looks something like

    [db]
    asyncpg=cockroachdb+asyncpg://root@localhost:26257/defaultdb
