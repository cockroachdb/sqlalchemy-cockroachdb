## Testing this dialect with SQLAlchemy and Alembic

"setup.cfg" contains a default SQLAlchemy connection URI that should
work if you have a local instance of CockroachDB installed:

    [db]
    default=cockroachdb://root@localhost:26257/defaultdb

If you want to test against a remote server (or otherwise need to tweak
the connection URI) simply create a file named "test.cfg" in the same
folder as "setup.cfg", copy the ``[db]`` section into it, and adjust the
``default=`` URI accordingly.

The minimum requirements for testing are:

- SQLAlchemy,
- Alembic,
- pytest, and
- the psycopg2 DBAPI module.

Install them with 

    pip install sqlalchemy alembic pytest psycopg2-binary

Then, to run a complete test simply invoke

    make test

at a command prompt after you bootstrapped your environment with 

    make bootstrap

To run just the SQLAlchemy test suite, use

    pytest test/test_suite_sqlalchemy.py

and to run just the Alembic test suite, use

    pytest test/test_suite_alembic.py

For more detailed information see the corresponding SQLAlchemy document

https://github.com/sqlalchemy/sqlalchemy/blob/master/README.unittests.rst
