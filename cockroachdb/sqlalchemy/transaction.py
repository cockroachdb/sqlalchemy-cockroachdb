from random import uniform
from time import sleep

import psycopg2
import psycopg2.errorcodes
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.orm

from .dialect import savepoint_state


def run_transaction(transactor, callback, max_retries=None, max_backoff=0):
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

    ``max_retries`` is an optional integer that specifies how many times the
    transaction should be retried before giving up.
    ``max_backoff`` is an optional integer that specifies the capped number of seconds
    for the exponential back-off.
    """
    if isinstance(transactor, sqlalchemy.engine.Connection):
        return _txn_retry_loop(transactor, callback, max_retries, max_backoff)
    elif isinstance(transactor, sqlalchemy.engine.Engine):
        with transactor.connect() as connection:
            return _txn_retry_loop(connection, callback, max_retries, max_backoff)
    elif isinstance(transactor, sqlalchemy.orm.sessionmaker):
        session = transactor(autocommit=True)
        return _txn_retry_loop(session, callback, max_retries, max_backoff)
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


def retry_exponential_backoff(retry_count: int, max_backoff: int = 0) -> None:
    """
    This is a function for an exponential back-off whenever we encounter a retry error.
    So we sleep for a bit before retrying,
    and the sleep time varies for each failed transaction
    capped by the max_backoff parameter.

    :param retry_count: The number for the current retry count
    :param max_backoff: The capped number of seconds for the exponential back-off
    :return: None
    """

    sleep_secs = uniform(0, min(max_backoff, 0.1 * (2 ** retry_count)))
    sleep(sleep_secs)


def _txn_retry_loop(conn, callback, max_retries, max_backoff):
    """Inner transaction retry loop.

    ``conn`` may be either a Connection or a Session, but they both
    have compatible ``begin()`` and ``begin_nested()`` methods.
    """
    retry_count = 0
    with conn.begin():
        while True:
            try:
                with _NestedTransaction(conn):
                    ret = callback(conn)
                    return ret
            except sqlalchemy.exc.DatabaseError as e:
                if max_retries is not None and retry_count >= max_retries:
                    raise
                retry_count += 1
                if isinstance(e.orig, psycopg2.OperationalError):
                    if e.orig.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                        if max_backoff > 0:
                            retry_exponential_backoff(retry_count, max_backoff)
                        continue
                raise
