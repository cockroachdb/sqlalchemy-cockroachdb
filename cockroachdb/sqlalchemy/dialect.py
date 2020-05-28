import collections
import re
import threading
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.util import warn
import sqlalchemy.sql as sql

import sqlalchemy.types as sqltypes

from .stmt_compiler import CockroachCompiler, CockroachIdentifierPreparer

# Map type names (as returned by information_schema) to sqlalchemy type
# objects.
#
# TODO(bdarnell): test more of these. The stock test suite only covers
# a few basic ones.
_type_map = {
    'bool': sqltypes.BOOLEAN,  # introspection returns "BOOL" not boolean
    'boolean': sqltypes.BOOLEAN,

    'bigint': sqltypes.INT,
    'int': sqltypes.INT,
    'int2': sqltypes.INT,
    'int4': sqltypes.INT,
    'int64': sqltypes.INT,
    'int8': sqltypes.INT,
    'integer': sqltypes.INT,
    'smallint': sqltypes.INT,

    'double precision': sqltypes.FLOAT,
    'float': sqltypes.FLOAT,
    'float4': sqltypes.FLOAT,
    'float8': sqltypes.FLOAT,
    'real': sqltypes.FLOAT,

    'dec': sqltypes.DECIMAL,
    'decimal': sqltypes.DECIMAL,
    'numeric': sqltypes.DECIMAL,

    'date': sqltypes.DATE,

    'time': sqltypes.Time,
    'time without time zone': sqltypes.Time,

    'timestamp': sqltypes.TIMESTAMP,
    'timestamptz': sqltypes.TIMESTAMP,
    'timestamp with time zone': sqltypes.TIMESTAMP,
    'timestamp without time zone': sqltypes.TIMESTAMP,

    'interval': sqltypes.Interval,

    'char': sqltypes.VARCHAR,
    'char varying': sqltypes.VARCHAR,
    'character': sqltypes.VARCHAR,
    'character varying': sqltypes.VARCHAR,
    'string': sqltypes.VARCHAR,
    'text': sqltypes.VARCHAR,
    'varchar': sqltypes.VARCHAR,

    'blob': sqltypes.BLOB,
    'bytea': sqltypes.BLOB,
    'bytes': sqltypes.BLOB,

    'json': sqltypes.JSON,
    'jsonb': sqltypes.JSON,

    'uuid': UUID,

    'inet': INET,
}


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
    supports_comments = False
    supports_sequences = False
    statement_compiler = CockroachCompiler
    preparer = CockroachIdentifierPreparer

    def __init__(self, *args, **kwargs):
        if kwargs.get("use_native_hstore", False):
            raise NotImplementedError("use_native_hstore is not supported")
        if kwargs.get("server_side_cursors", False):
            raise NotImplementedError("server_side_cursors is not supported")
        kwargs["use_native_hstore"] = False
        kwargs["server_side_cursors"] = False
        super(CockroachDBDialect, self).__init__(*args, **kwargs)

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
        sversion = connection.scalar("select version()")
        self._is_v2plus = " v1." not in sversion
        self._is_v21plus = self._is_v2plus and (" v2.0." not in sversion)
        self._is_v201plus = self._is_v21plus and (" v19." not in sversion)
        self._has_native_json = self._is_v2plus
        self._has_native_jsonb = self._is_v2plus
        self._supports_savepoints = self._is_v201plus

    def _get_server_version_info(self, conn):
        # PGDialect expects a postgres server version number here,
        # although we've overridden most of the places where it's
        # used.
        return (9, 5, 0)

    def get_table_names(self, conn, schema=None, **kw):
        # Upstream implementation needs correlated subqueries.

        if not self._is_v2plus:
            # v1.1 or earlier.
            return [row.Table for row in conn.execute("SHOW TABLES")]

        # v2.0+ have a good information schema. Use it.
        return [row.table_name for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=%s",
            (schema or self.default_schema_name,))]

    def has_table(self, conn, table, schema=None):
        # Upstream implementation needs pg_table_is_visible().
        return any(t == table for t in self.get_table_names(conn, schema=schema))

    # The upstream implementations of the reflection functions below depend on
    # correlated subqueries which are not yet supported.
    def get_columns(self, conn, table_name, schema=None, **kw):
        if not self._is_v2plus:
            # v1.1.
            # Bad: the table name is not properly escaped.
            # Oh well. Hoping 1.1 won't be around for long.
            rows = conn.execute('SHOW COLUMNS FROM "%s"."%s"' %
                                (schema or self.default_schema_name, table_name))
        else:
            # v2.0 or later. Information schema is usable.
            rows = conn.execute(
                'SELECT column_name, data_type, is_nullable::bool, column_default, '
                'numeric_precision, numeric_scale, character_maximum_length '
                'FROM information_schema.columns '
                'WHERE table_schema = %s AND table_name = %s AND NOT is_hidden::bool',
                (schema or self.default_schema_name, table_name),
            )

        res = []
        for row in rows:
            name, type_str, nullable, default = row[:4]
            # When there are type parameters, attach them to the
            # returned type object.
            m = re.match(r'^(\w+(?: \w+)*)(?:\(([0-9, ]*)\))?$', type_str)
            if m is None:
                warn("Could not parse type name '%s'" % type_str)
                typ = sqltypes.NULLTYPE
            else:
                type_name, type_args = m.groups()
                try:
                    type_class = _type_map[type_name.lower()]
                except KeyError:
                    warn("Did not recognize type '%s' of column '%s'" %
                         (type_name, name))
                    type_class = sqltypes.NULLTYPE
                if type_args:
                    typ = type_class(*[int(s.strip()) for s in type_args.split(',')])
                elif type_class is sqltypes.DECIMAL:
                    typ = type_class(
                        precision=row.numeric_precision,
                        scale=row.numeric_scale,
                    )
                elif type_class is sqltypes.VARCHAR:
                    typ = type_class(length=row.character_maximum_length)
                else:
                    typ = type_class
            res.append(dict(
                name=name,
                type=typ,
                nullable=nullable,
                default=default,
            ))
        return res

    def get_indexes(self, conn, table_name, schema=None, **kw):
        # The Cockroach database creates a UNIQUE INDEX implicitly whenever the
        # UNIQUE CONSTRAINT construct is used. Currently we are just ignoring all unique indexes,
        # but we might need to return them and add an additional key `duplicates_constraint` if
        # it is detected as mirroring a constraint.
        # https://www.cockroachlabs.com/docs/stable/unique.html
        # https://github.com/sqlalchemy/sqlalchemy/blob/55f930ef3d4e60bed02a2dad16e331fe42cfd12b/lib/sqlalchemy/dialects/postgresql/base.py#L723
        if not self._is_v2plus:
            q = '''
                SELECT
                    "Name" as index_name,
                    "Column" as column_name,
                    "Unique" as unique,
                    "Implicit" as implicit
                FROM
                    [SHOW INDEXES FROM "%(schema)s"."%(name)s"]
            '''
        else:
            q = '''
                SELECT
                    index_name,
                    column_name,
                    (not non_unique::bool) as unique,
                    implicit::bool as implicit
                FROM
                    information_schema.statistics
                WHERE
                    table_schema = %(schema)s
                    AND table_name = %(name)s
            '''
        rows = conn.execute(q, schema=(schema or self.default_schema_name), name=table_name)
        indexes = collections.defaultdict(list)
        for row in rows:
            if row.implicit or row.unique:
                continue
            indexes[row.index_name].append(row)

        result = []
        for name, rows in indexes.items():
            result.append({
                'name': name,
                'column_names': [r.column_name for r in rows],
                'unique': False,
            })
        return result

    def get_foreign_keys_v1(self, conn, table_name, schema=None, **kw):
        fkeys = []
        FK_REGEX = re.compile(
            r'(?P<referred_table>.+)?\.\[(?P<referred_columns>.+)?]')

        for row in conn.execute(
                'SHOW CONSTRAINTS FROM "%s"."%s"' %
                (schema or self.default_schema_name, table_name)):
            if row.Type.startswith("FOREIGN KEY"):
                m = re.search(FK_REGEX, row.Details)

                name = row.Name
                constrained_columns = row['Column(s)'].split(', ')
                referred_table = m.group('referred_table')
                referred_columns = m.group('referred_columns').split()
                referred_schema = schema
                fkey_d = {
                    'name': name,
                    'constrained_columns': constrained_columns,
                    'referred_table': referred_table,
                    'referred_columns': referred_columns,
                    'referred_schema': referred_schema
                }
                fkeys.append(fkey_d)
        return fkeys

    def get_foreign_keys(self, connection, table_name, schema=None,
                         postgresql_ignore_search_path=False, **kw):
        if not self._is_v2plus:
            # v1.1 or earlier.
            return self.get_foreign_keys_v1(connection, table_name, schema, **kw)

        # v2.0 or later.
        # This method is the same as the one in SQLAlchemy's pg dialect, with
        # a tweak to the FK regular expressions to tolerate whitespace between
        # the table name and the column list.
        # See also: https://github.com/cockroachdb/cockroach/issues/27123

        preparer = self.identifier_preparer
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        FK_SQL = """
          SELECT r.conname,
                pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
                n.nspname as conschema
          FROM  pg_catalog.pg_constraint r,
                pg_namespace n,
                pg_class c

          WHERE r.conrelid = :table AND
                r.contype = 'f' AND
                c.oid = confrelid AND
                n.oid = c.relnamespace
          ORDER BY 1
        """
        # http://www.postgresql.org/docs/9.0/static/sql-createtable.html
        FK_REGEX = re.compile(
            r'FOREIGN KEY \((.*?)\) REFERENCES (?:(.*?)\.)?(.*?)[\s]?\((.*?)\)'
            r'[\s]?(MATCH (FULL|PARTIAL|SIMPLE)+)?'
            r'[\s]?(ON UPDATE '
            r'(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?'
            r'[\s]?(ON DELETE '
            r'(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?'
            r'[\s]?(DEFERRABLE|NOT DEFERRABLE)?'
            r'[\s]?(INITIALLY (DEFERRED|IMMEDIATE)+)?'
        )

        t = sql.text(FK_SQL).columns(conname=sqltypes.Unicode,
                                     condef=sqltypes.Unicode)
        c = connection.execute(t, table=table_oid)
        fkeys = []
        for conname, condef, conschema in c.fetchall():
            m = re.search(FK_REGEX, condef).groups()

            constrained_columns, referred_schema, \
                referred_table, referred_columns, \
                _, match, _, onupdate, _, ondelete, \
                deferrable, _, initially = m

            if deferrable is not None:
                deferrable = True if deferrable == 'DEFERRABLE' else False
            constrained_columns = [preparer._unquote_identifier(x)
                                   for x in re.split(
                                       r'\s*,\s*', constrained_columns)]

            if postgresql_ignore_search_path:
                # when ignoring search path, we use the actual schema
                # provided it isn't the "default" schema
                if conschema != self.default_schema_name:
                    referred_schema = conschema
                else:
                    referred_schema = schema
            elif referred_schema:
                # referred_schema is the schema that we regexp'ed from
                # pg_get_constraintdef().  If the schema is in the search
                # path, pg_get_constraintdef() will give us None.
                referred_schema = \
                    preparer._unquote_identifier(referred_schema)
            elif schema is not None and schema == conschema:
                # If the actual schema matches the schema of the table
                # we're reflecting, then we will use that.
                referred_schema = schema

            referred_table = preparer._unquote_identifier(referred_table)
            referred_columns = [preparer._unquote_identifier(x)
                                for x in
                                re.split(r'\s*,\s', referred_columns)]
            fkey_d = {
                'name': conname,
                'constrained_columns': constrained_columns,
                'referred_schema': referred_schema,
                'referred_table': referred_table,
                'referred_columns': referred_columns,
                'options': {
                    'onupdate': onupdate,
                    'ondelete': ondelete,
                    'deferrable': deferrable,
                    'initially': initially,
                    'match': match
                }
            }
            fkeys.append(fkey_d)
        return fkeys

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        if self._is_v21plus:
            return super(CockroachDBDialect, self).get_pk_constraint(conn, table_name, schema, **kw)

        # v2.0 does not know about enough SQL to understand the query done by
        # the upstream dialect. So run a dumbed down version instead.
        idxs = self.get_indexes(conn, table_name, schema=schema, **kw)
        if len(idxs) == 0:
            # virtual table. No constraints.
            return {}
        # The PK is always first in the index list; it may not always
        # be named "primary".
        pk = idxs[0]
        res = dict(constrained_columns=pk["column_names"])
        # The SQLAlchemy tests expect that the name field is only
        # present if the PK was explicitly renamed by the user.
        # Checking for a name of "primary" is an imperfect proxy for
        # this but is good enough to pass the tests.
        if pk["name"] != "primary":
            res["name"] = pk["name"]
        return res

    def get_unique_constraints(self, conn, table_name, schema=None, **kw):
        if self._is_v21plus:
            return super(CockroachDBDialect, self).get_unique_constraints(
                conn, table_name, schema, **kw)

        # v2.0 does not know about enough SQL to understand the query done by
        # the upstream dialect. So run a dumbed down version instead.
        res = []
        # Skip the primary key which is always first in the list.
        idxs = self.get_indexes(conn, table_name, schema=schema, **kw)
        if len(idxs) == 0:
            # virtual table. No constraints.
            return res
        for index in idxs[1:]:
            if index["unique"]:
                del index["unique"]
                res.append(index)
        return res

    def get_check_constraints(self, conn, table_name, schema=None, **kw):
        if self._is_v21plus:
            return super(CockroachDBDialect, self).get_check_constraints(
                conn, table_name, schema, **kw)
        # TODO(bdarnell): The postgres dialect implementation depends on
        # pg_table_is_visible, which is supported in cockroachdb 1.1
        # but not in 1.0. Figure out a versioning strategy.
        return []

    def do_savepoint(self, connection, name):
        # Savepoint logic customized to work with run_transaction().
        if savepoint_state.cockroach_restart:
            connection.execute('SAVEPOINT cockroach_restart')
        else:
            super(CockroachDBDialect, self).do_savepoint(connection, name)

    def do_rollback_to_savepoint(self, connection, name):
        # Savepoint logic customized to work with run_transaction().
        if savepoint_state.cockroach_restart:
            connection.execute('ROLLBACK TO SAVEPOINT cockroach_restart')
        else:
            super(CockroachDBDialect, self).do_rollback_to_savepoint(connection, name)

    def do_release_savepoint(self, connection, name):
        # Savepoint logic customized to work with run_transaction().
        if savepoint_state.cockroach_restart:
            connection.execute('RELEASE SAVEPOINT cockroach_restart')
        else:
            super(CockroachDBDialect, self).do_release_savepoint(connection, name)


# If alembic is installed, register an alias in its dialect mapping.
try:
    import alembic.ddl.postgresql
except ImportError:
    pass
else:
    class CockroachDBImpl(alembic.ddl.postgresql.PostgresqlImpl):
        __dialect__ = 'cockroachdb'
        transactional_ddl = False

    @compiles(alembic.ddl.postgresql.PostgresqlColumnType, 'cockroachdb')
    def visit_column_type(*args, **kwargs):
        return alembic.ddl.postgresql.visit_column_type(*args, **kwargs)

# If sqlalchemy-migrate is installed, register there too.
try:
    from migrate.changeset.databases.visitor import DIALECTS as migrate_dialects
except ImportError:
    pass
else:
    migrate_dialects['cockroachdb'] = migrate_dialects['postgresql']
