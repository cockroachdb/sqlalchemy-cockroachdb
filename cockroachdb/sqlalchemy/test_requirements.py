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

    # We don't support these features yet, but the tests have them on
    # by default.
    temporary_tables = exclusions.closed()
    temp_table_reflection = exclusions.closed()
    time = exclusions.skip_if(lambda config: not config.db.dialect._is_v2plus,
                              "v1.x does not support TIME.")
    time_microseconds = exclusions.skip_if(lambda config: not config.db.dialect._is_v2plus,
                                           "v1.x does not support TIME.")
    timestamp_microseconds = exclusions.open()
    server_side_cursors = exclusions.closed()

    # We don't do implicit casts.
    date_coerces_from_datetime = exclusions.closed()

    # We do not support creation of views with `SELECT *` expressions,
    # which these tests use.
    view_reflection = exclusions.closed()
    view_column_reflection = exclusions.closed()

    # The autoincrement tests assume a predictable 1-based sequence.
    autoincrement_insert = exclusions.closed()

    # The following features are off by default. We turn on as many as
    # we can without causing test failures.
    table_reflection = exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                                          "older versions don't support this correctly.")
    primary_key_constraint_reflection = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                           "older versions don't support this correctly.")
    foreign_key_constraint_reflection = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                           "older versions don't support this correctly.")
    index_reflection = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                           "older versions don't support this correctly.")
    unique_constraint_reflection = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                           "older versions don't support this correctly.")
    # TODO: enable after 20.2 beta comes out
    # check_constraint_reflection = \
    #        exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
    #                           "older versions don't support this correctly.")
    cross_schema_fk_reflection = exclusions.closed()
    non_updating_cascade = exclusions.open()
    deferrable_fks = exclusions.closed()
    boolean_col_expressions = exclusions.open()
    nullsordering = exclusions.open()
    standalone_binds = exclusions.open()
    intersect = exclusions.open()
    except_ = exclusions.open()
    window_functions = exclusions.open()
    empty_inserts = exclusions.open()
    returning = exclusions.open()
    multivalues_inserts = exclusions.open()
    emulated_lastrowid = exclusions.open()
    dbapi_lastrowid = exclusions.open()
    views = exclusions.open()
    schemas = exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                                 "versions before 20.2 do not suport schemas")
    implicit_default_schema = exclusions.skip_if(lambda config: not config.db.dialect._is_v202plus,
                                                 "versions before 20.2 do not suport schemas")
    sequences = exclusions.closed()
    sequences_optional = exclusions.closed()
    temporary_views = exclusions.closed()
    reflects_pk_names = exclusions.open()
    unicode_ddl = exclusions.open()
    datetime_literals = exclusions.closed()
    datetime_historic = exclusions.open()
    date_historic = exclusions.open()
    precision_numerics_enotation_small = exclusions.open()
    precision_numerics_enotation_large = exclusions.open()
    precision_numerics_many_significant_digits = exclusions.open()
    precision_numerics_retains_significant_digits = exclusions.closed()
    savepoints = exclusions.skip_if(lambda config: not config.db.dialect._supports_savepoints,
                                    "versions before 20.x do not support savepoints.")
    two_phase_transactions = exclusions.closed()
    update_from = exclusions.open()
    mod_operator_as_percent_sign = exclusions.open()
    foreign_key_constraint_reflection = exclusions.open()
    computed_columns = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v191plus,
                           "versions before 19.1 do not support reflection on computed columns")
    computed_columns_stored = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v191plus,
                           "versions before 19.1 do not support reflection on computed columns")
    computed_columns_default_persisted = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v191plus,
                           "versions before 19.1 do not support reflection on computed columns")
    computed_columns_reflect_persisted = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v191plus,
                           "versions before 19.1 do not support reflection on computed columns")
    computed_columns_virtual = exclusions.closed()
    ctes = exclusions.skip_if(lambda config: not config.db.dialect._is_v201plus,
                              "versions before 20.x do not fully support CTEs.")
    ctes_with_update_delete = \
        exclusions.skip_if(lambda config: not config.db.dialect._is_v201plus,
                           "versions before 20.x do not fully support CTEs.")
    ctes_on_dml = exclusions.skip_if(lambda config: not config.db.dialect._is_v201plus,
                                     "versions before 20.x do not fully support CTEs.")
    isolation_level = exclusions.open()
    json_type = exclusions.skip_if(lambda config: not config.db.dialect._is_v192plus,
                                   "versions before 19.2.x do not pass the JSON tests.")
    tuple_in = exclusions.open()
    # The psycopg driver doesn't support these.
    percent_schema_names = exclusions.closed()
    order_by_label_with_expression = exclusions.open()
    order_by_col_from_union = exclusions.open()
    implicitly_named_constraints = exclusions.open()

    def get_isolation_levels(self, config):
        return {
            "default": "SERIALIZABLE",
            "supported": [
                "SERIALIZABLE"
            ]
        }
