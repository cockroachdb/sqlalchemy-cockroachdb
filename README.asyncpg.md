## asyncpg support

The connection URL is of the form:

    cockroachdb+asyncpg://root@localhost:26257/defaultdb

There is a customized version of the FastAPI SQL database tutorial for
`cockroachdb+asyncpg` available at

https://github.com/gordthompson/fastapi-tutorial-cockroachdb-async

### Testing

Assuming that you have an entry in test.cfg that looks something like

    [db]
    asyncpg=cockroachdb+asyncpg://root@localhost:26257/defaultdb

you can run the tests with asyncpg using a command like

    pytest --db=asyncpg

If you want to run all the tests *except* the Alembic tests then invoke pytest
using a command like

    pytest --db=asyncpg --ignore-glob='*test_suite_alembic.py'
