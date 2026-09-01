# Version 2.0.5
Unreleased

- Re-work get_multi_columns() to 
  - include identity column info (#297), and
  - avoid parse error when reflecting ENUMs (#303).  
  (CRDB 26.3+ required for full compatibility.)

# Version 2.0.4
April 23, 2026

- Fix reflection of CHAR columns (#275)
- Fix reflection of TIMESTAMPTZ columns (#276), thanks to @nvachhar
- Fix reflection of JSONB columns (#277)
- Fix compatibility issues with Alembic 1.18 (via SQLA 2.0.47)
- Update minimum Python version to 3.10
- Compile MySQL-style `func.timestampdiff(unit, start, end)` to a
  PostgreSQL-style `EXTRACT(EPOCH FROM ...)` expression on the cockroachdb
  dialect. The arithmetic result is wrapped in `TRUNC()` so the value matches
  MySQL's integer-truncation-toward-zero semantics (a 90-second diff at
  `MINUTE` returns 1, not 1.5), and is cast to NUMERIC so callers may safely
  combine it with integer or numeric divisors -- avoiding the `float / decimal`
  arithmetic errors CockroachDB rejects but PostgreSQL accepts. Supported
  units: MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK.
  Calendar-aware units (MONTH, QUARTER, YEAR) are explicitly rejected with a
  specific error message because they require calendar walking that cannot be
  derived from epoch arithmetic alone. Enables cross-dialect ORMs (e.g.
  Apache Airflow) that fall back to `timestampdiff` for non-PostgreSQL
  backends.


# Version 2.0.3
June 10, 2025

- Add support for READ COMMITTED transaction isolation
  (see [README.read_committed.md](README.read_committed.md))
- Add column comment to get_columns method (#253), thanks to @dotan-mor
- Fix autogenerate with ON UPDATE / DELETE (#258, #262), thanks to @idumitrescu-dn
- Improve support for table/column comments (via SQLA 2.0.36)
- Add nested transaction support (#267), thanks to @mfmarche

# Version 2.0.2
January 10, 2024

- Implement reflection for array types (#213)
- Fix get_multi_columns() to support multiple table names in filter_array (#220)
- Enhance foreign key reflection to accommodate quoting differences with PostgreSQL
- Add psycopg (v3) support (#185)
- Remove unconditional import of psycopg2 (#176)

# Version 2.0.1
April 14, 2023

- Enable AUTOCOMMIT isolation_level for SQLA 2.0 (#205)

# Version 2.0.0
Released February 21, 2023

- Applied dialect code and test changes for compatibility with SQLAlchemy 2.0. This
  version of the dialect requires SQLAlchemy 2.0, so to work with earlier versions of
  SQLAlchemy use `pip install sqlalchemy-cockroachdb<2.0.0`
- Stopped sending telemetry data during startup.
