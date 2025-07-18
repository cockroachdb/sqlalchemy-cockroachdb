import collections
import re
import threading
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.util import warn
import sqlalchemy.sql as sql

import sqlalchemy.types as sqltypes

from .stmt_compiler import CockroachCompiler, CockroachIdentifierPreparer
from .ddl_compiler import CockroachDDLCompiler


# Map type names (as returned by information_schema) to sqlalchemy type
# objects.
#
# TODO(bdarnell): test more of these. The stock test suite only covers
# a few basic ones.
_type_map = {
    "bool": sqltypes.BOOLEAN,  # introspection returns "BOOL" not boolean
    "boolean": sqltypes.BOOLEAN,
    "bigint": sqltypes.INT,
    "int": sqltypes.INT,
    "int2": sqltypes.INT,
    "int4": sqltypes.INT,
    "int64": sqltypes.INT,
    "int8": sqltypes.INT,
    "integer": sqltypes.INT,
    "smallint": sqltypes.INT,
    "double precision": sqltypes.FLOAT,
    "float": sqltypes.FLOAT,
    "float4": sqltypes.FLOAT,
    "float8": sqltypes.FLOAT,
    "real": sqltypes.FLOAT,
    "dec": sqltypes.DECIMAL,
    "decimal": sqltypes.DECIMAL,
    "numeric": sqltypes.DECIMAL,
    "date": sqltypes.DATE,
    "time": sqltypes.Time,
    "time without time zone": sqltypes.Time,
    "timestamp": sqltypes.TIMESTAMP,
    "timestamptz": sqltypes.TIMESTAMP,
    "timestamp with time zone": sqltypes.TIMESTAMP,
    "timestamp without time zone": sqltypes.TIMESTAMP,
    "interval": sqltypes.Interval,
    "char": sqltypes.VARCHAR,
    "char varying": sqltypes.VARCHAR,
    "character": sqltypes.VARCHAR,
    "character varying": sqltypes.VARCHAR,
    "string": sqltypes.VARCHAR,
    "text": sqltypes.VARCHAR,
    "varchar": sqltypes.VARCHAR,
    "blob": sqltypes.BLOB,
    "bytea": sqltypes.BLOB,
    "bytes": sqltypes.BLOB,
    "json": sqltypes.JSON,
    "jsonb": sqltypes.JSON,
    "uuid": UUID,
    "inet": INET,
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


class CockroachDBDialect(PGDialect):
    name = "cockroachdb"
    supports_empty_insert = True
    supports_multivalues_insert = True
    supports_sequences = False
    statement_compiler = CockroachCompiler
    preparer = CockroachIdentifierPreparer
    ddl_compiler = CockroachDDLCompiler

    # Override connect so we can take disable_cockroachdb_telemetry as a connect_arg to sqlalchemy.
    # The option is not used any more, but removing it is a backwards-incompatible change.
    def connect(
        self,
        disable_cockroachdb_telemetry=False,
        **kwargs,
    ):
        return super().connect(**kwargs)

    def __init__(self, *args, **kwargs):
        if kwargs.get("use_native_hstore", False):
            raise NotImplementedError("use_native_hstore is not supported")
        if kwargs.get("server_side_cursors", False):
            raise NotImplementedError("server_side_cursors is not supported")
        kwargs["use_native_hstore"] = False
        kwargs["server_side_cursors"] = False
        super().__init__(*args, **kwargs)

    def initialize(self, connection):
        # Bypass PGDialect's initialize implementation, which looks at
        # server_version_info and performs postgres-specific queries
        # to detect certain features on the server. Set the attributes
        # by hand and hope things don't change out from under us too
        # often.
        super().initialize(connection)
        self.implicit_returning = True
        self.supports_smallserial = False
        self._backslash_escapes = False
        sversion = connection.scalar(text("select version()"))
        self._is_v2plus = " v1." not in sversion
        self._is_v21plus = self._is_v2plus and (" v2.0." not in sversion)
        self._is_v191plus = self._is_v21plus and (" v2.1." not in sversion)
        self._is_v192plus = self._is_v191plus and (" v19.1." not in sversion)
        self._is_v201plus = self._is_v192plus and (" v19.2." not in sversion)
        self._is_v202plus = self._is_v201plus and (" v20.1." not in sversion)
        self._is_v211plus = self._is_v202plus and (" v20.2." not in sversion)
        self._is_v212plus = self._is_v211plus and (" v21.1." not in sversion)
        self._is_v221plus = self._is_v212plus and (" v21.2." not in sversion)
        self._is_v222plus = self._is_v221plus and (" v22.1." not in sversion)
        self._is_v231plus = self._is_v222plus and (" v22.2." not in sversion)
        self._is_v232plus = self._is_v231plus and (" v23.1." not in sversion)
        self._is_v241plus = self._is_v232plus and (" v23.2." not in sversion)
        self._is_v242plus = self._is_v241plus and (" v24.1." not in sversion)
        self._is_v243plus = self._is_v242plus and (" v24.2." not in sversion)
        self._is_v251plus = self._is_v243plus and (" v24.3." not in sversion)
        self._is_v252plus = self._is_v251plus and (" v25.1." not in sversion)
        self._has_native_json = self._is_v2plus
        self._has_native_jsonb = self._is_v2plus
        self._supports_savepoints = self._is_v201plus
        self.supports_native_enum = self._is_v202plus
        self.supports_identity_columns = True

    def _get_server_version_info(self, conn):
        # PGDialect expects a postgres server version number here,
        # although we've overridden most of the places where it's
        # used.
        return (9, 5, 0)

    def get_table_names(self, conn, schema=None, **kw):
        # Upstream implementation needs correlated subqueries.

        if not self._is_v2plus:
            # v1.1 or earlier.
            return [row.Table for row in conn.execute(text("SHOW TABLES"))]

        # v2.0+ have a good information schema. Use it.
        return [
            row.table_name
            for row in conn.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema=:schema"),
                {"schema": schema or self.default_schema_name},
            )
        ]

    def has_table(self, conn, table, schema=None, info_cache=None):
        # Upstream implementation needs pg_table_is_visible().
        return any(t == table for t in self.get_table_names(conn, schema=schema))

    def get_multi_columns(self, connection, schema, filter_names, scope, kind, **kw):
        if not filter_names:
            filter_names = self.get_table_names(connection, schema)
        return {
            (schema, table_name): self.get_columns(connection, table_name, schema, **kw)
            for table_name in filter_names
        }

    # The upstream implementations of the reflection functions below depend on
    # correlated subqueries which are not yet supported.
    def get_columns(self, conn, table_name, schema=None, **kw):
        _include_hidden = kw.get("include_hidden", False)
        if not self._is_v191plus:
            # v2.x does not have is_generated or generation_expression
            sql = (
                "SELECT column_name, data_type, is_nullable::bool, column_default,"
                "numeric_precision, numeric_scale, character_maximum_length, "
                "NULL AS is_generated, NULL AS generation_expression, is_hidden::bool,"
                "column_comment AS comment "
                "FROM information_schema.columns "
                "WHERE table_schema = :table_schema AND table_name = :table_name "
            )
            sql += "" if _include_hidden else "AND NOT is_hidden::bool"
            rows = conn.execute(
                text(sql),
                {"table_schema": schema or self.default_schema_name, "table_name": table_name},
            )
        else:
            # v19.1 or later. Information schema columns are all usable.
            sql = (
                "SELECT column_name, data_type, is_nullable::bool, column_default, "
                "numeric_precision, numeric_scale, character_maximum_length, "
                "CASE is_generated WHEN 'ALWAYS' THEN true WHEN 'NEVER' THEN false "
                "ELSE is_generated::bool END AS is_generated, "
                "generation_expression, is_hidden::bool, crdb_sql_type, column_comment AS comment "
                "FROM information_schema.columns "
                "WHERE table_schema = :table_schema AND table_name = :table_name "
            )
            sql += "" if _include_hidden else "AND NOT is_hidden::bool"
            rows = conn.execute(
                text(sql),
                {"table_schema": schema or self.default_schema_name, "table_name": table_name},
            )

        res = []
        for row in rows:
            name, type_str, nullable, default = row[:4]
            if type_str == "ARRAY":
                is_array = True
                type_str, _ = row.crdb_sql_type.split("[", maxsplit=1)
            else:
                is_array = False
            # When there are type parameters, attach them to the
            # returned type object.
            m = re.match(r"^(\w+(?: \w+)*)(?:\(([0-9, ]*)\))?$", type_str)
            if m is None:
                warn("Could not parse type name '%s'" % type_str)
                typ = sqltypes.NULLTYPE
            else:
                type_name, type_args = m.groups()
                try:
                    type_class = _type_map[type_name.lower()]
                except KeyError:
                    warn(f"Did not recognize type '{type_name}' of column '{name}'")
                    type_class = sqltypes.NULLTYPE
                if type_args:
                    typ = type_class(*[int(s.strip()) for s in type_args.split(",")])
                elif type_class is sqltypes.DECIMAL:
                    typ = type_class(
                        precision=row.numeric_precision,
                        scale=row.numeric_scale,
                    )
                elif type_class is sqltypes.VARCHAR:
                    typ = type_class(length=row.character_maximum_length)
                else:
                    typ = type_class
            if row.is_generated:
                # Currently, all computed columns are persisted.
                computed = dict(sqltext=row.generation_expression, persisted=True)
                default = None
            else:
                computed = None
            # Check if a sequence is being used and adjust the default value.
            autoincrement = False
            if default is not None:
                nextval_match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
                unique_rowid_match = re.search(r"""unique_rowid\(""", default)
                if nextval_match is not None or unique_rowid_match is not None:
                    if issubclass(type_class, sqltypes.Integer):
                        autoincrement = True
                    # the default is related to a Sequence
                    sch = schema
                    if (
                        nextval_match is not None
                        and "." not in nextval_match.group(2)
                        and sch is not None
                    ):
                        # unconditionally quote the schema name.  this could
                        # later be enhanced to obey quoting rules /
                        # "quote schema"
                        default = (
                            nextval_match.group(1)
                            + ('"%s"' % sch)
                            + "."
                            + nextval_match.group(2)
                            + nextval_match.group(3)
                        )

            column_info = dict(
                name=name,
                type=ARRAY(typ) if is_array else typ,
                nullable=nullable,
                default=default,
                autoincrement=autoincrement,
                is_hidden=row.is_hidden,
                comment=row.comment,
            )
            if computed is not None:
                column_info["computed"] = computed
            res.append(column_info)
        return res

    def get_indexes(self, conn, table_name, schema=None, **kw):
        if self._is_v192plus:
            indexes = super().get_indexes(conn, table_name, schema, **kw)
            # CockroachDB creates a UNIQUE INDEX automatically for each UNIQUE CONSTRAINT, and
            # there is no difference between unique indexes and unique constraints.  We need
            # to remove the `duplicates_constraints` value from unique indexes, otherwise
            # alembic tries to delete and recreate unique indexes.  This is consistent with
            # postgresql which doesn't set the duplicates_constraint flag on unique indexes
            for index in indexes:
                if index["unique"] and "duplicates_constraint" in index:
                    del index["duplicates_constraint"]
            return indexes

        # The Cockroach database creates a UNIQUE INDEX implicitly whenever the
        # UNIQUE CONSTRAINT construct is used. Currently we are just ignoring all unique indexes,
        # but we might need to return them and add an additional key `duplicates_constraint` if
        # it is detected as mirroring a constraint.
        # https://www.cockroachlabs.com/docs/stable/unique.html
        # https://github.com/sqlalchemy/sqlalchemy/blob/55f930ef3d4e60bed02a2dad16e331fe42cfd12b/lib/sqlalchemy/dialects/postgresql/base.py#L723
        q = """
            SELECT
                index_name,
                column_name,
                (not non_unique::bool) as unique,
                implicit::bool as implicit
            FROM
                information_schema.statistics
            WHERE
                table_schema = :table_schema
                AND table_name = :table_name
        """
        rows = conn.execute(
            text(q),
            {"table_schema": (schema or self.default_schema_name), "table_name": table_name},
        )
        indexes = collections.defaultdict(list)
        for row in rows:
            if row.implicit or row.unique:
                continue
            indexes[row.index_name].append(row)

        result = []
        for name, rows in indexes.items():
            result.append(
                {
                    "name": name,
                    "column_names": [r.column_name for r in rows],
                    "unique": False,
                }
            )
        return result

    def get_multi_indexes(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        result = super().get_multi_indexes(
            connection, schema, filter_names, scope, kind, **kw
        )
        if schema is None:
            result = dict(result)
            for k in [
                (None, "spatial_ref_sys"),
                (None, "geometry_columns"),
                (None, "geography_columns"),
            ]:
                result.pop(k, None)
        return result

    def get_foreign_keys_v1(self, conn, table_name, schema=None, **kw):
        fkeys = []
        FK_REGEX = re.compile(r"(?P<referred_table>.+)?\.\[(?P<referred_columns>.+)?]")

        for row in conn.execute(
            text(f'SHOW CONSTRAINTS FROM "{schema or self.default_schema_name}"."{table_name}"')
        ):
            if row.Type.startswith("FOREIGN KEY"):
                m = re.search(FK_REGEX, row.Details)

                name = row.Name
                constrained_columns = row["Column(s)"].split(", ")
                referred_table = m.group("referred_table")
                referred_columns = m.group("referred_columns").split()
                referred_schema = schema
                fkey_d = {
                    "name": name,
                    "constrained_columns": constrained_columns,
                    "referred_table": referred_table,
                    "referred_columns": referred_columns,
                    "referred_schema": referred_schema,
                }
                fkeys.append(fkey_d)
        return fkeys

    @util.memoized_property
    def _fk_regex_pattern(self):
        # optionally quoted token
        qtoken = r'(?:"[^"]+"|[\w]+?)'

        # https://www.postgresql.org/docs/current/static/sql-createtable.html
        return re.compile(
            r"FOREIGN KEY \((.*?)\) "
            rf"REFERENCES (?:({qtoken})\.)?({qtoken})\(((?:{qtoken}(?: *, *)?)+)\)"  # noqa: E501
            r"[\s]?(MATCH (FULL|PARTIAL|SIMPLE)+)?"
            r"[\s]?(ON DELETE "
            r"(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?"
            r"[\s]?(ON UPDATE "
            r"(CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT)+)?"
            r"[\s]?(DEFERRABLE|NOT DEFERRABLE)?"
            r"[\s]?(INITIALLY (DEFERRED|IMMEDIATE)+)?"
        )

    def get_foreign_keys(
        self, connection, table_name, schema=None, postgresql_ignore_search_path=False, **kw
    ):
        if not self._is_v2plus:
            # v1.1 or earlier.
            return self.get_foreign_keys_v1(connection, table_name, schema, **kw)

        # v2.0 or later.
        # This method is the same as the one in SQLAlchemy's pg dialect, with
        # a tweak to the FK regular expressions to tolerate whitespace between
        # the table name and the column list.
        # See also: https://github.com/cockroachdb/cockroach/issues/27123

        preparer = self.identifier_preparer
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

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
        FK_REGEX = self._fk_regex_pattern

        t = sql.text(FK_SQL).columns(conname=sqltypes.Unicode, condef=sqltypes.Unicode)
        c = connection.execute(t, {"table": table_oid})
        fkeys = []
        for conname, condef, conschema in c.fetchall():
            m = re.search(FK_REGEX, condef).groups()

            (
                constrained_columns,
                referred_schema,
                referred_table,
                referred_columns,
                _,
                match,
                _,
                ondelete,
                _,
                onupdate,
                deferrable,
                _,
                initially,
            ) = m

            if deferrable is not None:
                deferrable = True if deferrable == "DEFERRABLE" else False
            constrained_columns = [
                preparer._unquote_identifier(x) for x in re.split(r"\s*,\s*", constrained_columns)
            ]

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
                referred_schema = preparer._unquote_identifier(referred_schema)
            elif schema is not None and schema == conschema:
                # If the actual schema matches the schema of the table
                # we're reflecting, then we will use that.
                referred_schema = schema

            referred_table = preparer._unquote_identifier(referred_table)
            referred_columns = [
                preparer._unquote_identifier(x) for x in re.split(r"\s*,\s", referred_columns)
            ]
            fkey_d = {
                "name": conname,
                "constrained_columns": constrained_columns,
                "referred_schema": referred_schema,
                "referred_table": referred_table,
                "referred_columns": referred_columns,
                "options": {
                    "onupdate": onupdate,
                    "ondelete": ondelete,
                    "deferrable": deferrable,
                    "initially": initially,
                    "match": match,
                },
            }
            fkeys.append(fkey_d)
        return fkeys

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        if self._is_v21plus:
            return super().get_pk_constraint(conn, table_name, schema, **kw)

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

    def get_multi_pk_constraint(self, connection, schema, filter_names, scope, kind, **kw):
        result = super().get_multi_pk_constraint(
            connection, schema, filter_names, scope, kind, **kw
        )
        if schema is None:
            result = dict(result)
            for k in [
                (None, "spatial_ref_sys"),
                (None, "geometry_columns"),
                (None, "geography_columns"),
            ]:
                result.pop(k, None)
        return result

    def get_unique_constraints(self, conn, table_name, schema=None, **kw):
        if self._is_v21plus:
            return super().get_unique_constraints(conn, table_name, schema, **kw)

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

    def get_multi_check_constraints(
        self, connection, schema, filter_names, scope, kind, **kw
    ):
        result = super().get_multi_check_constraints(
            connection, schema, filter_names, scope, kind, **kw
        )
        if schema is None:
            result = dict(result)
            for k in [
                (None, "spatial_ref_sys"),
                (None, "geometry_columns"),
                (None, "geography_columns"),
            ]:
                result.pop(k, None)
        return result

    def do_savepoint(self, connection, name):
        # Savepoint logic customized to work with run_transaction().
        if savepoint_state.cockroach_restart:
            connection.execute(text("SAVEPOINT cockroach_restart"))
        else:
            super().do_savepoint(connection, name)

    def do_rollback_to_savepoint(self, connection, name):
        # Savepoint logic customized to work with run_transaction().
        if savepoint_state.cockroach_restart:
            connection.execute(text("ROLLBACK TO SAVEPOINT cockroach_restart"))
        else:
            super().do_rollback_to_savepoint(connection, name)

    def do_release_savepoint(self, connection, name):
        # Savepoint logic customized to work with run_transaction().
        if savepoint_state.cockroach_restart:
            connection.execute(text("RELEASE SAVEPOINT cockroach_restart"))
        else:
            super().do_release_savepoint(connection, name)


# If alembic is installed, register an alias in its dialect mapping.
try:
    import alembic.ddl.postgresql
except ImportError:
    pass
else:

    class CockroachDBImpl(alembic.ddl.postgresql.PostgresqlImpl):
        __dialect__ = "cockroachdb"
        transactional_ddl = False

    @compiles(alembic.ddl.postgresql.PostgresqlColumnType, "cockroachdb")
    def visit_column_type(*args, **kwargs):
        return alembic.ddl.postgresql.visit_column_type(*args, **kwargs)

    @compiles(alembic.ddl.postgresql.ColumnComment, "cockroachdb")
    def visit_column_comment(*args, **kwargs):
        return alembic.ddl.postgresql.visit_column_comment(*args, **kwargs)


# If sqlalchemy-migrate is installed, register there too.
try:
    from migrate.changeset.databases.visitor import DIALECTS as migrate_dialects
except ImportError:
    pass
else:
    migrate_dialects["cockroachdb"] = migrate_dialects["postgresql"]
