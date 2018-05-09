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
