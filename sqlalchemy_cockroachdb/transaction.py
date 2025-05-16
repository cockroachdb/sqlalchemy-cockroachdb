from random import uniform
from time import sleep

import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.orm

from .base import savepoint_state


class ChainTransaction:
    def __init__(self, transactions=None):
        self.results = []
        self.transactions = transactions or []

    def add_result(self, result):
        self.results.append(result)


def run_transaction(transactor, callback, max_retries=None, max_backoff=0, **kwargs):
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
    ``inject_error`` forces retry loop to run via SET inject_retry_errors_enabled = 'true'
    ``use_cockroach_restart``, default true, utilizes the special cockroach_restart protocol,
    as outlined in: https://www.cockroachlabs.com/blog/nested-transactions-in-cockroachdb-20-1/
    """
    if isinstance(transactor, (sqlalchemy.engine.Connection, sqlalchemy.orm.Session)):
        return _txn_retry_loop(transactor, callback, max_retries, max_backoff, **kwargs)
    elif isinstance(transactor, sqlalchemy.engine.Engine):
        with transactor.connect() as connection:
            return _txn_retry_loop(connection, callback, max_retries, max_backoff, **kwargs)
    elif isinstance(transactor, sqlalchemy.orm.sessionmaker):
        session = transactor()
        return _txn_retry_loop(session, callback, max_retries, max_backoff, **kwargs)
    else:
        raise TypeError("don't know how to run a transaction on %s", type(transactor))


class _NestedTransaction:
    """Wraps begin_nested() to set the savepoint_state thread-local.

    This causes the savepoint statements that are a part of this retry
    loop to be rewritten by the dialect.
    """

    def __init__(self, conn, use_cockroach_restart=True):
        self.conn = conn
        self.use_cockroach_restart = use_cockroach_restart

    def __enter__(self):
        try:
            if self.use_cockroach_restart:
                savepoint_state.cockroach_restart = True
            self.txn = self.conn.begin_nested()
            if self.use_cockroach_restart and isinstance(self.conn, sqlalchemy.orm.Session):
                # Sessions are lazy and don't execute the savepoint
                # query until you ask for the connection.
                self.conn.connection()
        finally:
            if self.use_cockroach_restart:
                savepoint_state.cockroach_restart = False
        return self

    def __exit__(self, typ, value, tb):
        try:
            if self.use_cockroach_restart:
                savepoint_state.cockroach_restart = True
            self.txn.__exit__(typ, value, tb)
        finally:
            if self.use_cockroach_restart:
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

    sleep_secs = uniform(0, min(max_backoff, 0.1 * (2**retry_count)))
    sleep(sleep_secs)


def run_in_nested_transaction(
    conn, callback, max_retries, max_backoff, inject_error=False, **kwargs
):
    if isinstance(conn, sqlalchemy.orm.Session):
        dbapi_name = conn.bind.driver
    else:
        dbapi_name = conn.engine.driver

    retry_count = 0
    while True:
        if inject_error and retry_count == 0:
            conn.execute(sqlalchemy.text("SET inject_retry_errors_enabled = 'true'"))
        elif inject_error:
            conn.execute(sqlalchemy.text("SET inject_retry_errors_enabled = 'false'"))
        try:
            with _NestedTransaction(conn, **kwargs):
                return callback(conn)
        except sqlalchemy.exc.DatabaseError as e:
            if max_retries is not None and retry_count >= max_retries:
                raise
            do_retry = False
            if dbapi_name == "psycopg2":
                import psycopg2
                import psycopg2.errorcodes

                if isinstance(e.orig, psycopg2.OperationalError):
                    if e.orig.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                        do_retry = True
            else:
                import psycopg

                if isinstance(e.orig, psycopg.errors.SerializationFailure):
                    do_retry = True
            if do_retry:
                retry_count += 1
                if max_backoff > 0:
                    retry_exponential_backoff(retry_count, max_backoff)
                continue
            raise


def _txn_retry_loop(conn, callback, max_retries, max_backoff, **kwargs):
    """Inner transaction retry loop.

    ``conn`` may be either a Connection or a Session, but they both
    have compatible ``begin()`` and ``begin_nested()`` methods.
    """
    with conn.begin():
        result = run_in_nested_transaction(conn, callback, max_retries, max_backoff, **kwargs)
        if isinstance(result, ChainTransaction):
            for transaction in result.transactions:
                result.add_result(
                    run_in_nested_transaction(conn, transaction, max_retries, max_backoff, **kwargs)
                )
        return result
