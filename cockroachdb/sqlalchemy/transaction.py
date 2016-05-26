import psycopg2
import psycopg2.errorcodes
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.orm

from .dialect import savepoint_state


def run_transaction(transactor, callback):
    """Run a transaction with retries.

    ``callback()`` will be called with one argument to execute the
    transaction. ``callback`` may be called more than once; it should have
    no side effects other than writes to the database on the given
    connection. ``callback`` should not call ``commit()` or ``rollback()``;
    these will be called automatically.

    The ``transactor`` argument may be one of the following types:
    * `sqlalchemy.engine.Connection`: the same connection is passed to the callback.
    * `sqlalchemy.engine.Engine`: a connection is created and passed to the callback.
    * `sqlalchemy.orm.sessionmaker`: a session is created and passed to the callback.
    """
    if isinstance(transactor, sqlalchemy.engine.Connection):
        return _txn_retry_loop(transactor, callback)
    elif isinstance(transactor, sqlalchemy.engine.Engine):
        with transactor.connect() as connection:
            return _txn_retry_loop(connection, callback)
    elif isinstance(transactor, sqlalchemy.orm.sessionmaker):
        session = transactor(autocommit=True)
        return _txn_retry_loop(session, callback)
    else:
        raise TypeError("don't know how to run a transaction on %s", type(transactor))


class _NestedTransaction(object):
    """Wraps begin_nested() to set the savepoint_state thread-local.

    This causes the savepoint statements that are a part of this retry
    loop to be rewritten by the dialect.
    """
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        try:
            savepoint_state.cockroach_restart = True
            self.txn = self.conn.begin_nested()
            if isinstance(self.conn, sqlalchemy.orm.Session):
                # Sessions are lazy and don't execute the savepoint
                # query until you ask for the connection.
                self.conn.connection()
        finally:
            savepoint_state.cockroach_restart = False
        return self

    def __exit__(self, typ, value, tb):
        try:
            savepoint_state.cockroach_restart = True
            self.txn.__exit__(typ, value, tb)
        finally:
            savepoint_state.cockroach_restart = False


def _txn_retry_loop(conn, callback):
    """Inner transaction retry loop.

    ``conn`` may be either a Connection or a Session, but they both
    have compatible ``begin()`` and ``begin_nested()`` methods.
    """
    with conn.begin():
        while True:
            try:
                with _NestedTransaction(conn):
                    ret = callback(conn)
                    return ret
            except sqlalchemy.exc.DatabaseError as e:
                if isinstance(e.orig, psycopg2.OperationalError):
                    if e.orig.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                        continue
                raise
