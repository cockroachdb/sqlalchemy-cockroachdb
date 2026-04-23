from sqlalchemy.dialects.postgresql.base import PGCompiler
from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import BindParameter
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
    "WEEK": " / 604800",
}

# Calendar-aware units are intentionally not implemented. MySQL's
# ``TIMESTAMPDIFF(MONTH, ...)`` walks the calendar so that ``Feb 28 -> Mar 1``
# is one month while ``Mar 1 -> Mar 30`` is zero months. That logic cannot be
# derived from epoch arithmetic; a faithful implementation would need
# ``EXTRACT(YEAR FROM AGE(end, start))`` plus month math. Listing them here
# lets us emit a specific error rather than the generic "unsupported unit" one.
_TIMESTAMPDIFF_CALENDAR_AWARE_UNITS = frozenset({"MONTH", "QUARTER", "YEAR"})


def _resolve_timestampdiff_unit(unit_arg, compiler, **kwargs):
    """Extract the unit token from a ``timestampdiff()`` first argument.

    The unit must be known at compile time so it can be turned into a SQL
    arithmetic factor. Plain Python strings (``func.timestampdiff("SECOND", ...)``)
    and ``literal("SECOND")`` reach the compiler as :class:`BindParameter`; if we
    delegated to ``compiler.process`` those would render as parameter
    placeholders (``$1`` / ``%(...)s``), so we extract ``.value`` directly.
    Other constructs such as ``text("SECOND")`` and ``literal_column("SECOND")``
    render as literal SQL tokens and go through the normal path.
    """
    if isinstance(unit_arg, BindParameter):
        raw = unit_arg.value
        if not isinstance(raw, str):
            raise ValueError(
                "timestampdiff() unit must be a string; " f"got {type(raw).__name__} ({raw!r})"
            )
        return raw.strip().upper()
    return compiler.process(unit_arg, **kwargs).strip().strip("'\"").upper()


@compiles(timestampdiff, "cockroachdb")
def _compile_timestampdiff_cockroachdb(element, compiler, **kwargs):
    """Compile ``timestampdiff(unit, start, end)`` for the cockroachdb dialect.

    Output shape::

        TRUNC(CAST(EXTRACT(EPOCH FROM (end - start)) AS NUMERIC) <factor>)

    The ``TRUNC()`` wrap matches MySQL's ``TIMESTAMPDIFF`` semantics: the
    result is the integer count of complete units between the two timestamps,
    truncated toward zero. Without it, a 90-second diff at ``MINUTE`` would
    return ``1.5`` on cockroachdb where MySQL returns ``1``.

    The cast to ``NUMERIC`` (rather than to ``BIGINT``) is intentional. It
    keeps the value integer-truncated like MySQL while still allowing
    downstream divisors -- e.g. Apache Airflow's
    ``timestampdiff(MICROSECOND, ...) / 1_000_000`` pattern -- to do
    floating-point division on cockroachdb. Returning ``BIGINT`` would force
    integer division on the divisor and silently lose sub-second precision.

    Calendar-aware units (``MONTH``, ``QUARTER``, ``YEAR``) are intentionally
    rejected with a specific error; see ``_TIMESTAMPDIFF_CALENDAR_AWARE_UNITS``
    for the rationale.
    """
    args = list(element.clauses)
    if len(args) != 3:
        raise ValueError(f"timestampdiff() expects 3 arguments (unit, start, end); got {len(args)}")
    unit_token = _resolve_timestampdiff_unit(args[0], compiler, **kwargs)
    if unit_token in _TIMESTAMPDIFF_CALENDAR_AWARE_UNITS:
        raise ValueError(
            f"timestampdiff() unit {unit_token!r} is not supported on the cockroachdb "
            "dialect. Calendar-aware units (MONTH, QUARTER, YEAR) require calendar "
            "walking (e.g. Feb 28 -> Mar 1 is 1 month) that cannot be derived from "
            "epoch arithmetic alone, and are intentionally omitted. "
            "If you need them, please open an issue at "
            "https://github.com/cockroachdb/sqlalchemy-cockroachdb/issues."
        )
    if unit_token not in _TIMESTAMPDIFF_UNIT_FACTOR:
        raise ValueError(
            f"Unsupported timestampdiff() unit for cockroachdb dialect: {unit_token!r}. "
            f"Supported units: {sorted(_TIMESTAMPDIFF_UNIT_FACTOR)}. "
            "Pass the unit as a plain string, sqlalchemy.literal(unit), "
            "or sqlalchemy.text(unit)."
        )
    start_expr = compiler.process(args[1], **kwargs)
    end_expr = compiler.process(args[2], **kwargs)
    epoch_diff = f"CAST(EXTRACT(EPOCH FROM ({end_expr} - {start_expr})) AS NUMERIC)"
    factor = _TIMESTAMPDIFF_UNIT_FACTOR[unit_token]
    return f"TRUNC({epoch_diff}{factor})"
