# Version 1.4.4
Unreleased

- Added `include_hidden` option to `get_columns()` to enable reflection of columns like "rowid". (#173)
- Added support for `.with_hint()` (patch courtesy of Jonathan Dieter)

# Version 1.4.3
Released December 10, 2021

- Added preliminary support for asyncpg. See instructions in the README.

# Version 1.4.2
Released October 21, 2021

- Updated version telemetry to only report major/minor version of SQLAlchemy.

# Version 1.4.1
Released October 12, 2021

- Updated test suite to work with Alembic 1.7.

# Version 1.4.0
Released July 29, 2021

- Add telemetry to SQLAlchemy CockroachDB
- Telemetry is enabled by default, set disable_cockroachdb_telemetry in create_engine's connect_args field to disable.
  - ```Example: engine = create_engine('cockroachdb://...', connect_args={"disable_cockroachdb_telemetry": True})```
- Initial compatibility with SQLAlchemy 1.4.


# Version 1.3.3
Released April 26, 2021

- Remove `duplicates_constraint` property for unique indexes

# Version 1.3.2
Released September 29, 2020

- Stopped returning primary keys in get_indexes. (#42)
- Enabled tests for enums and user-defined schemas for CockroachDB v20.2.

# Version 1.3.1
Released July 13, 2020

- Added more support computed columns. (#119)
- Enabled more tests from SQLAlchemy test suite in CI.

# Version 1.3.0

Released June 10, 2020

- Removed python2 support.
- Version number increased to 1.3.0 to indicate compatibility with SQLAlchemy 1.3.x.
- Column type changes via Alembic are now allowed. (#96)
- Added exponential backoff to run_transaction(). (#115)

# Version 0.4.0

Released April 10, 2020

- Renamed package to sqlalchemy-cockroachdb.

# Version 0.3.3

Released October 28, 2019

- Fixed error when the use_native_hstore or server_side_cursors keyword
  arguments were specified.
- Stopped using the deprecated sql.text.typemap parameter.

# Version 0.3.2

Released July 1, 2019

- Removed requirement for psycopg2 so psycopg2-binary can be used as well.
- Updated urllib3 to remove security vulnerability.

# Version 0.3.1

Released Feb 25, 2019

- Support CockroachDB version numbers greater than 2.

# Version 0.3.0

Released Jan 23, 2019

- Added support for more data types.
- Improved introspection of types with modifiers (decimal, varchar).
- Improved introspection of unique constraints.

# Version 0.2.1

Released Aug 16, 2018

- Alembic migrations no longer attempt to run DDL statements in transactions.
- Comments are now dropped from table definitions as CockroachDB does not support them.

# Version 0.2.0

Released July 16, 2018

- Adapter again simultaneously compatible with CockroachDB 1.1, 2.0
  and 2.1.

# Version 0.1.5

Released July 10, 2018

- More compatibility improvements for JSON/JSONB support.

# Version 0.1.4

Released May 9, 2018

- Improved compatibility of JSON/JSONB support.

# Version 0.1.3

Released Mar 27, 2018

- Support for JSONB columns is now reported in accordance with CockroachDB 2.0.

# Version 0.1.2

Released Feb 7, 2018

- If Alembic or `sqlalchemy-migrate` is installed, an experimental
  `cockroachdb` dialect will be registered with those packages too.
- The `get_foreign_keys()` introspection interface is now supported.
- Fixed introspection of boolean columns.

# Version 0.1.1

Released Sep 28, 2017

- Works with CockroachDB 1.0 and 1.1.
- `get_foreign_keys()` reflection is stubbed out and always returns an empty list.
- Reflection interfaces and the `RETURNING` clause support references to tables outside the current schema.
- Foreign key constraints are no longer stripped out of table creation.

# Version 0.1.0

Released May 27, 2016

- Initial release
