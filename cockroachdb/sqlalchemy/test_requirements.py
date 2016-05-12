from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    # This class configures the sqlalchemy test suite. Oddly, it must
    # be importable in the main codebase and not alongside the tests.
    #
    # The full list of supported settings is at
    # https://github.com/zzzeek/sqlalchemy/blob/master/lib/sqlalchemy/testing/requirements.py

    # This one's undocumented but appears to control connection reuse
    # in the tests.
    independent_connections = exclusions.open()

    # We don't support these features yet.
    foreign_key_constraint_reflection = exclusions.closed()
    temporary_tables = exclusions.closed()
    temp_table_reflection = exclusions.closed()
    time = exclusions.closed()
    time_microseconds = exclusions.closed()

    # We don't do implicit casts.
    date_coerces_from_datetime = exclusions.closed()

    # Our reflection support is incomplete (we need to return type
    # parameters and hide the implicit rowid index).
    table_reflection = exclusions.closed()

    # The autoincrement tests assume a predictable 1-based sequence.
    autoincrement_insert = exclusions.closed()

    # Turn on all the settings that are off by default but we support.
    boolean_col_expressions = exclusions.open()
    nullsordering = exclusions.open()
    standalone_binds = exclusions.open()
    intersect = exclusions.open()
    empty_inserts = exclusions.open()
    multivalues_inserts = exclusions.open()
    emulated_lastrowid = exclusions.open()
    dbapi_lastrowid = exclusions.open()
    reflects_pk_names = exclusions.open()
    unicode_ddl = exclusions.open()
    datetime_historic = exclusions.open()
    date_historic = exclusions.open()
    update_from = exclusions.open()
    mod_operator_as_percent_sign = exclusions.open()
    order_by_label_with_expression = exclusions.open()
