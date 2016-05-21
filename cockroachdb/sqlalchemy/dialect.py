import collections
import threading
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.util import warn

import sqlalchemy.types as sqltypes

from .ddl_compiler import DDLCompiler
from .type_compiler import TypeCompiler

# Map type names (as returned by SHOW COLUMNS) to sqlalchemy type
# objects.
#
# TODO(bdarnell): test more of these. The stock test suite only covers
# a few basic ones.
_type_map = dict(
    boolean=sqltypes.BOOLEAN,
    int=sqltypes.INT,
    integer=sqltypes.INT,
    smallint=sqltypes.INT,
    bigint=sqltypes.INT,
    float=sqltypes.FLOAT,
    real=sqltypes.FLOAT,
    double=sqltypes.FLOAT,
    decimal=sqltypes.DECIMAL,
    numeric=sqltypes.DECIMAL,
    date=sqltypes.DATE,
    timestamp=sqltypes.TIMESTAMP,
    timestamptz=sqltypes.TIMESTAMP,
    interval=sqltypes.Interval,
    string=sqltypes.VARCHAR,
    char=sqltypes.VARCHAR,
    varchar=sqltypes.VARCHAR,
    bytes=sqltypes.BLOB,
    blob=sqltypes.BLOB,
)


class _SavepointState(threading.local):
    """Hack to override names used in savepoint statements.

    To get the Session to do the right thing with transaction retries,
    we use the begin_nested() method, which executes a savepoint. We
    need to transform the savepoint statements that are a part of this
    retry loop, while leaving other savepoints alone. Unfortunately
    the interface leaves us with no way to pass this information along
    except via a thread-local variable.
    """
    def __init__(self):
        self.cockroach_restart = False
savepoint_state = _SavepointState()


class CockroachDBDialect(PGDialect_psycopg2):
    name = 'cockroachdb'
    supports_sequences = False
    ddl_compiler = DDLCompiler
    type_compiler = TypeCompiler

    def __init__(self, *args, **kwargs):
        super(CockroachDBDialect, self).__init__(*args,
                                                 use_native_hstore=False,
                                                 server_side_cursors=False,
                                                 **kwargs)

    def initialize(self, connection):
        # Bypass PGDialect's initialize implementation, which looks at
        # server_version_info and performs postgres-specific queries
        # to detect certain features on the server. Set the attributes
        # by hand and hope things don't change out from under us too
        # often.
        super(PGDialect, self).initialize(connection)
        self.implicit_returning = True
        self.supports_native_enum = False
        self.supports_smallserial = False
        self._backslash_escapes = False

    def _get_server_version_info(self, conn):
        # PGDialect expects a postgres server version number here,
        # although we've overridden most of the places where it's
        # used.
        return (9, 5, 0)

    def _get_default_schema_name(self, conn):
        raise NotImplementedError()

    def get_table_names(self, conn, schema=None, **kw):
        return [row.Table for row in conn.execute("SHOW TABLES")]

    def has_table(self, conn, table, schema=None):
        return any(t == table for t in self.get_table_names(conn, schema=schema))

    def get_columns(self, conn, table_name, schema=None, **kw):
        res = []
        # TODO(bdarnell): escape table name
        for row in conn.execute('SHOW COLUMNS FROM "%s"' % table_name):
            name, type_name, nullable, default = row
            # TODO(bdarnell): when there are type parameters, attach
            # them to the returned type object.
            try:
                typ = _type_map[type_name.split('(')[0].lower()]
            except KeyError:
                warn("Did not recognize type '%s' of column '%s'" % (type_name, name))
                typ = sqltypes.NULLTYPE
            res.append(dict(
                name=name,
                type=typ,
                nullable=nullable,
                default=default,
            ))
        return res

    def get_indexes(self, conn, table_name, schema=None, **kw):
        # Maps names to a bool indicating whether the index is unique.
        uniques = collections.OrderedDict()
        columns = collections.defaultdict(list)
        # TODO(bdarnell): escape table name
        for row in conn.execute('SHOW INDEXES FROM "%s"' % table_name):
            columns[row.Name].append(row.Column)
            uniques[row.Name] = row.Unique
        res = []
        # Map over uniques because it preserves order.
        for name in uniques:
            res.append(dict(name=name, column_names=columns[name], unique=uniques[name]))
        return res

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        # The PK is always first in the index list; it may not always
        # be named "primary".
        pk = self.get_indexes(conn, table_name, schema=schema, **kw)[0]
        res = dict(constrained_columns=pk["column_names"])
        # The SQLAlchemy tests expect that the name field is only
        # present if the PK was explicitly renamed by the user.
        # Checking for a name of "primary" is an imperfect proxy for
        # this but is good enough to pass the tests.
        if pk["name"] != "primary":
            res["name"] = pk["name"]
        return res

    def get_unique_constraints(self, conn, table_name, schema=None, **kw):
        res = []
        # Skip the primary key which is always first in the list.
        for index in self.get_indexes(conn, table_name, schema=schema, **kw)[1:]:
            if index["unique"]:
                del index["unique"]
                res.append(index)
        return res

    def do_savepoint(self, connection, name):
        if savepoint_state.cockroach_restart:
            connection.execute('SAVEPOINT cockroach_restart')
        else:
            super(CockroachDBDialect, self).do_savepoint(connection, name)

    def do_rollback_to_savepoint(self, connection, name):
        if savepoint_state.cockroach_restart:
            connection.execute('ROLLBACK TO SAVEPOINT cockroach_restart')
        else:
            super(CockroachDBDialect, self).do_rollback_to_savepoint(connection, name)

    def do_release_savepoint(self, connection, name):
        if savepoint_state.cockroach_restart:
            connection.execute('RELEASE SAVEPOINT cockroach_restart')
        else:
            super(CockroachDBDialect, self).do_release_savepoint(connection, name)
