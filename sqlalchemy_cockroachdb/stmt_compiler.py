from sqlalchemy.dialects.postgresql.base import PGCompiler
from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import GenericFunction

# This is extracted from CockroachDB's `sql.y`. Add keywords here if *NEW* reserved keywords
# are added to sql.y. DO NOT DELETE keywords here, even if they are deleted from sql.y:
# once a keyword in CockroachDB, forever a keyword in clients (because of cross-version compat).
crdb_grammar_reserved = """
  ALL
| ANALYSE
| ANALYZE
| AND
| ANY
| ARRAY
| AS
| ASC
| ASYMMETRIC
| BOTH
| CASE
| CAST
| CHECK
| COLLATE
| COLUMN
| CONSTRAINT
| CREATE
| CURRENT_CATALOG
| CURRENT_DATE
| CURRENT_ROLE
| CURRENT_SCHEMA
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| DEFAULT
| DEFERRABLE
| DESC
| DISTINCT
| DO
| ELSE
| END
| EXCEPT
| FALSE
| FETCH
| FOR
| FOREIGN
| FROM
| GRANT
| GROUP
| HAVING
| IN
| INDEX
| INITIALLY
| INTERSECT
| INTO
| LATERAL
| LEADING
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| NOT
| NOTHING
| NULL
| OFFSET
| ON
| ONLY
| OR
| ORDER
| PLACING
| PRIMARY
| REFERENCES
| RETURNING
| ROLE
| SELECT
| SESSION_USER
| SOME
| SYMMETRIC
| TABLE
| THEN
| TO
| TRAILING
| TRUE
| UNION
| UNIQUE
| USER
| USING
| VARIADIC
| VIEW
| VIRTUAL
| WHEN
| WHERE
| WINDOW
| WITH
| WORK
"""
CRDB_RESERVED_WORDS = {x.strip().lower() for x in crdb_grammar_reserved.split("|")}


class CockroachIdentifierPreparer(PGIdentifierPreparer):
    reserved_words = CRDB_RESERVED_WORDS


class CockroachCompiler(PGCompiler):
    def format_from_hint_text(self, sqltext, table, hint, iscrud):
        return f"{sqltext}@{hint}"


class timestampdiff(GenericFunction):
    """MySQL-style ``timestampdiff(unit, start, end)`` for cross-dialect SQL.

    CockroachDB does not implement MySQL's ``timestampdiff()``. Applications
    that target multiple database backends (notably Apache Airflow's ORM)
    sometimes call ``func.timestampdiff(...)`` and rely on the database to
    accept it. Registering this :class:`GenericFunction` lets the cockroachdb
    statement compiler translate the call into a PostgreSQL-style
    ``EXTRACT(EPOCH FROM (end - start))`` expression.
    """

    inherit_cache = True
    name = "timestampdiff"


_TIMESTAMPDIFF_UNIT_FACTOR = {
    "MICROSECOND": " * 1000000",
    "MILLISECOND": " * 1000",
    "SECOND": "",
    "MINUTE": " / 60",
    "HOUR": " / 3600",
    "DAY": " / 86400",
}


@compiles(timestampdiff, "cockroachdb")
def _compile_timestampdiff_cockroachdb(element, compiler, **kwargs):
    """Compile ``timestampdiff(unit, start, end)`` for the cockroachdb dialect.

    The result is cast to ``NUMERIC`` so callers may safely combine it with
    integer or numeric divisors. CockroachDB rejects ``float / decimal``
    arithmetic that PostgreSQL accepts, and ``EXTRACT(EPOCH FROM ...)``
    returns a float on CockroachDB.
    """
    args = list(element.clauses)
    if len(args) != 3:
        raise ValueError(f"timestampdiff() expects 3 arguments (unit, start, end); got {len(args)}")
    unit_token = compiler.process(args[0], **kwargs).strip().strip("'\"").upper()
    if unit_token not in _TIMESTAMPDIFF_UNIT_FACTOR:
        raise ValueError(
            f"Unsupported timestampdiff() unit for cockroachdb dialect: {unit_token!r}. "
            f"Supported units: {sorted(_TIMESTAMPDIFF_UNIT_FACTOR)}"
        )
    start_expr = compiler.process(args[1], **kwargs)
    end_expr = compiler.process(args[2], **kwargs)
    epoch_diff = f"CAST(EXTRACT(EPOCH FROM ({end_expr} - {start_expr})) AS NUMERIC)"
    factor = _TIMESTAMPDIFF_UNIT_FACTOR[unit_token]
    return f"({epoch_diff}{factor})" if factor else epoch_diff
