## Testing this dialect with SQLAlchemy

"setup.cfg" contains a default SQLAlchemy connection URI that should
work if you have a local instance of CockroachDB installed:

    [db]
    default=cockroachdb://root@localhost:26257/defaultdb?disable_cockroachdb_telemetry=True

If you want to test against a remote server (or otherwise need to tweak
the connection URI) simply create a file named "test.cfg" in the same
folder as "setup.cfg", copy the ``[db]`` section into it, and adjust the
``default=`` URI accordingly.

The minimum requirements for testing are:

- SQLAlchemy,
- pytest, and
- the psycopg2 DBAPI module.

Install them with 

    pip install sqlalchemy pytest psycopg2-binary

Then, to run a complete test simply invoke

    pytest

at a command prompt in your virtual environment.

For more detailed information see the corresponding SQLAlchemy document

https://github.com/sqlalchemy/sqlalchemy/blob/master/README.unittests.rst