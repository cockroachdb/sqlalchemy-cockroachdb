import psycopg2
import sqlalchemy.exc


def run_transaction(conn, func):
    """Run a transaction on the given connection.

    ``func()`` will be called with one argument (the connection) to
    execute the transaction. ``func`` may be called more than once; it
    should have no side effects other than writes to the database on
    the given connection.
    """
    with conn.begin():
        while True:
            conn.execute("SAVEPOINT cockroach_restart")
            try:
                ret = func(conn)
                conn.execute("RELEASE SAVEPOINT cockroach_restart")
                return ret
            except sqlalchemy.exc.DatabaseError as e:
                if isinstance(e.orig, psycopg2.DatabaseError):
                    if e.orig.pgcode == 'CR000':
                        conn.execute("ROLLBACK TO SAVEPOINT cockroach_restart")
                        continue
                raise
