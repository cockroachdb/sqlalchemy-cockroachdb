import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import TIMESTAMP
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect
from sqlalchemy.sql import func
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
        ],
    )
    def test_compiles_with_arithmetic_suffix(self, unit, expected_suffix):
        expr = func.timestampdiff(text(unit), self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "AS NUMERIC" in sql
        assert expected_suffix in sql

    def test_seconds_has_no_arithmetic_suffix(self):
        expr = func.timestampdiff(text("SECOND"), self.events.c.start_date, self.events.c.end_date)
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "AS NUMERIC" in sql
        after_cast = sql.split("AS NUMERIC", 1)[1]
        assert " * " not in after_cast
        assert " / " not in after_cast

    def test_lowercase_unit_accepted(self):
        expr = func.timestampdiff(
            text("microsecond"), self.events.c.start_date, self.events.c.end_date
        )
        sql = _compile(select(expr), self.dialect)
        assert "EXTRACT(EPOCH FROM" in sql
        assert "* 1000000" in sql

    def test_unknown_unit_rejected(self):
        expr = func.timestampdiff(
            text("FORTNIGHT"), self.events.c.start_date, self.events.c.end_date
        )
        with pytest.raises(ValueError, match="Unsupported timestampdiff"):
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
        assert "timestampdiff" in sql.lower()

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
        assert "%(" not in sql
        assert "$1" not in sql

    def test_non_string_bind_value_rejected_clearly(self):
        """A BindParameter whose value isn't a string must produce a clear error."""
        expr = func.timestampdiff(123, self.events.c.start_date, self.events.c.end_date)
        with pytest.raises(ValueError, match="must be a string"):
            _compile(select(expr), self.dialect, literal_binds=False)
