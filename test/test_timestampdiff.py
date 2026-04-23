from datetime import datetime

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import TIMESTAMP
from sqlalchemy import bindparam
from sqlalchemy import literal
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.sql import func
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures

from sqlalchemy_cockroachdb.psycopg2 import CockroachDBDialect_psycopg2
from sqlalchemy_cockroachdb.stmt_compiler import timestampdiff  # noqa: F401  registers compiler


def _events_table():
    metadata = MetaData()
    return Table(
        "events",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("start_date", TIMESTAMP),
        Column("end_date", TIMESTAMP),
    )


def _compile(stmt, dialect, literal_binds=True):
    kwargs = {"literal_binds": True} if literal_binds else {}
    return str(stmt.compile(dialect=dialect, compile_kwargs=kwargs))


class TimestampdiffCompilerTest(fixtures.TestBase):
    """Compile-only tests: no live database connection required."""

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dialect = CockroachDBDialect_psycopg2()
        self.events = _events_table()

    @pytest.mark.parametrize(
        "unit,expected_suffix",
        [
            ("MICROSECOND", "* 1000000"),
            ("MILLISECOND", "* 1000"),
            ("MINUTE", "/ 60"),
            ("HOUR", "/ 3600"),
            ("DAY", "/ 86400"),
            ("WEEK", "/ 604800"),
        ],
    )
    def test_compiles_with_arithmetic_suffix(self, unit, expected_suffix):
        expr = func.timestampdiff(text(unit), self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "AS NUMERIC" in sql
        assert expected_suffix in sql
        assert "TRUNC(" in sql

    def test_seconds_has_no_arithmetic_suffix(self):
        """SECOND has no factor token but is still wrapped in TRUNC for MySQL parity."""
        expr = func.timestampdiff(text("SECOND"), self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "AS NUMERIC" in sql
        assert "TRUNC(" in sql
        after_cast = sql.split("AS NUMERIC", 1)[1]
        assert " * " not in after_cast
        assert " / " not in after_cast

    def test_truncates_to_match_mysql_semantics(self):
        """A 90-second diff at MINUTE must compile to a TRUNC()-wrapped expression
        so the value matches MySQL's integer-truncation semantics. See compiler
        docstring for the full rationale.
        """
        expr = func.timestampdiff(text("MINUTE"), self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), self.dialect)
        # Single anchored assertion: cast closes, factor applies, TRUNC closes.
        # A regression that emitted ``TRUNC(CAST(...)) / 60`` would still satisfy
        # weaker checks like ``"/ 60)" in sql``; the anchored fragment below
        # rejects that shape.
        assert "AS NUMERIC) / 60)" in sql, f"TRUNC must wrap the division; got SQL: {sql!r}"

    def test_lowercase_unit_accepted(self):
        expr = func.timestampdiff(
            text("microsecond"), self.events.c.start_date, self.events.c.end_date
        )
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "* 1000000" in sql
        assert "TRUNC(" in sql

    def test_unknown_unit_rejected(self):
        expr = func.timestampdiff(
            text("FORTNIGHT"), self.events.c.start_date, self.events.c.end_date
        )
        with pytest.raises(ValueError, match="Unsupported timestampdiff"):
            _compile(select(expr), self.dialect)

    @pytest.mark.parametrize("unit", ["MONTH", "QUARTER", "YEAR", "month", "Year"])
    def test_calendar_aware_units_rejected_with_explanation(self, unit):
        """MONTH/QUARTER/YEAR must be rejected with a specific error explaining
        why they're intentionally omitted (calendar-walking vs epoch arithmetic),
        not the generic 'unsupported unit' error.
        """
        expr = func.timestampdiff(text(unit), self.events.c.start_date, self.events.c.end_date)
        with pytest.raises(ValueError, match="Calendar-aware units"):
            _compile(select(expr), self.dialect)

    def test_wrong_arity_rejected(self):
        expr = func.timestampdiff(text("SECOND"), self.events.c.start_date)
        with pytest.raises(ValueError, match="3 arguments"):
            _compile(select(expr), self.dialect)

    def test_postgresql_dialect_unaffected(self):
        """The cockroachdb compiler hook must not change rendering for other dialects."""
        expr = func.timestampdiff(text("SECOND"), self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), postgresql_dialect())
        assert "EXTRACT" not in sql
        # Anchor on the call form rather than substring; "timestampdifference" or
        # similar names would otherwise pass.
        assert "timestampdiff(" in sql.lower()

    def test_plain_string_unit_accepted(self):
        """Plain Python string unit must resolve to a real unit, not a bound placeholder.

        Compiles WITHOUT literal_binds to mirror how Airflow's ORM actually executes
        statements — BindParameters render as ``$1`` / ``%(...)s`` unless we extract
        the value at compile time.
        """
        expr = func.timestampdiff("MICROSECOND", self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), self.dialect, literal_binds=False)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "* 1000000" in sql
        assert "TRUNC(" in sql
        assert "%(" not in sql
        assert "$1" not in sql

    def test_literal_unit_accepted(self):
        """``literal('SECOND')`` should resolve via BindParameter value, not a placeholder."""
        expr = func.timestampdiff(
            literal("SECOND"), self.events.c.start_date, self.events.c.end_date
        )
        sql = _compile(select(expr), self.dialect, literal_binds=False)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "AS NUMERIC" in sql
        assert "TRUNC(" in sql
        assert "%(" not in sql
        assert "$1" not in sql

    def test_non_string_bind_value_rejected_clearly(self):
        """A BindParameter whose value isn't a string must produce a clear error."""
        expr = func.timestampdiff(123, self.events.c.start_date, self.events.c.end_date)
        with pytest.raises(ValueError, match="must be a string"):
            _compile(select(expr), self.dialect, literal_binds=False)

    def test_bindparam_unit_accepted(self):
        """An explicit ``bindparam(name, value)`` should resolve via the BindParameter
        path, the same as plain strings and ``literal()``.
        """
        expr = func.timestampdiff(
            bindparam("u", "SECOND"),
            self.events.c.start_date,
            self.events.c.end_date,
        )
        sql = _compile(select(expr), self.dialect, literal_binds=False)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "TRUNC(" in sql
        assert "%(" not in sql
        assert "$1" not in sql

    def test_whitespace_in_text_unit_stripped(self):
        """``text(' SECOND ')`` should resolve to ``SECOND`` after .strip()."""
        expr = func.timestampdiff(
            text(" SECOND "), self.events.c.start_date, self.events.c.end_date
        )
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "TRUNC(" in sql

    def test_null_unit_rejected(self):
        """``null()`` as the unit argument must produce a clear error rather than
        silently rendering ``NULL`` as a unit token.
        """
        expr = func.timestampdiff(null(), self.events.c.start_date, self.events.c.end_date)
        with pytest.raises(ValueError, match="Unsupported timestampdiff"):
            _compile(select(expr), self.dialect)


class TimestampdiffBackendTest(fixtures.TablesTest):
    """Live-database tests against CockroachDB.

    Compile-only tests above verify the SQL string shape; these tests verify
    the *value* semantics by executing the compiled queries against a live
    cockroachdb instance. Without these, a future change that altered
    compilation (e.g. swapping ``TRUNC()`` for ``FLOOR()``) could pass every
    compile-time test while diverging from MySQL's actual behavior.
    """

    __backend__ = True
    __only_on__ = "cockroachdb"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "events_for_timestampdiff",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("start_date", TIMESTAMP),
            Column("end_date", TIMESTAMP),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.events_for_timestampdiff.insert(),
            [
                # 90 seconds positive: MySQL TIMESTAMPDIFF(MINUTE) = 1, not 1.5.
                dict(
                    id=1,
                    start_date=datetime(2026, 1, 1, 0, 0, 0),
                    end_date=datetime(2026, 1, 1, 0, 1, 30),
                ),
                # 90 seconds negative (start > end): MySQL = -1 because TIMESTAMPDIFF
                # truncates toward zero. A FLOOR()-based implementation would return
                # -2 here while still passing every positive-diff test.
                dict(
                    id=2,
                    start_date=datetime(2026, 1, 1, 0, 1, 30),
                    end_date=datetime(2026, 1, 1, 0, 0, 0),
                ),
                # 1.5 seconds positive: feeds the Airflow division-pattern test.
                dict(
                    id=3,
                    start_date=datetime(2026, 1, 1, 0, 0, 0),
                    end_date=datetime(2026, 1, 1, 0, 0, 1, 500000),
                ),
            ],
        )

    def test_minute_diff_truncates_positive(self, connection):
        events = self.tables.events_for_timestampdiff
        expr = func.timestampdiff(text("MINUTE"), events.c.start_date, events.c.end_date)
        result = connection.execute(select(expr).where(events.c.id == 1)).scalar()
        eq_(int(result), 1)

    def test_minute_diff_truncates_negative_toward_zero(self, connection):
        """Pins TRUNC-toward-zero (not FLOOR). For a -90 second diff at MINUTE:
        TRUNC(-1.5) = -1, FLOOR(-1.5) = -2. MySQL returns -1.
        """
        events = self.tables.events_for_timestampdiff
        expr = func.timestampdiff(text("MINUTE"), events.c.start_date, events.c.end_date)
        result = connection.execute(select(expr).where(events.c.id == 2)).scalar()
        eq_(int(result), -1)

    def test_airflow_microsecond_division_pattern(self, connection):
        """Airflow's exact pattern: timestampdiff(MICROSECOND, start, end) / 1_000_000.

        Confirms the NUMERIC return type does NOT trigger cockroachdb's
        ``unsupported binary operator: <float> / <decimal>`` rejection that
        an unwrapped ``EXTRACT(EPOCH FROM ...)`` (a float) would. The cast
        to NUMERIC inside the compilation is what makes this work.
        """
        events = self.tables.events_for_timestampdiff
        microsec = func.timestampdiff(text("MICROSECOND"), events.c.start_date, events.c.end_date)
        seconds = microsec / 1_000_000
        result = connection.execute(select(seconds).where(events.c.id == 3)).scalar()
        # 1.5s diff -> 1500000 microseconds (TRUNCated) / 1_000_000 -> 1.5
        eq_(float(result), 1.5)
