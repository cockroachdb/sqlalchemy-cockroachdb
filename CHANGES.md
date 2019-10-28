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
