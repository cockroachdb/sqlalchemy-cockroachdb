from sqlalchemy import __version__ as sa_version
from sqlalchemy.testing import skip
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _ComponentReflectionTestExtra
from sqlalchemy.testing.suite import CompositeKeyReflectionTest as _CompositeKeyReflectionTest
from sqlalchemy.testing.suite import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import TrueDivTest as _TrueDivTest
from sqlalchemy.testing.suite import UnicodeSchemaTest as _UnicodeSchemaTest


class ComponentReflectionTest(_ComponentReflectionTest):
    @testing.combinations(
        (False,), (True, testing.requires.schemas), argnames="use_schema"
    )
    @testing.requires.foreign_key_constraint_reflection
    def test_get_foreign_keys(self, connection, use_schema):
        if connection.dialect.driver != "asyncpg":
            super().test_get_foreign_keys(connection, use_schema, None)

    def test_get_indexes(self, connection):
        if (
            connection.dialect.driver != "asyncpg"
            and connection.dialect._is_v202plus
            and sa_version >= "1.4"
        ):
            super().test_get_indexes(connection, None, None)

    @skip("cockroachdb")  # noqa
    def test_get_noncol_index(self):
        # test not designed to handle ('desc', 'nulls_last')
        # ... also, fails with asyncpg re:
        # https://github.com/cockroachdb/cockroach/issues/71908
        pass

    @skip("cockroachdb")  # noqa
    def test_get_multi_columns(self):
        pass

    @skip("cockroachdb")  # noqa
    def test_get_multi_indexes(self):
        pass

    @skip("cockroachdb")  # noqa
    def test_get_multi_pk_constraint(self):
        pass

    @skip("cockroachdb")  # noqa
    def test_get_pk_constraint(self):
        # we still have a "rowid" constraint when no explicit PK declared
        pass

    @skip("cockroachdb")  # noqa
    def test_get_view_names(self):
        # TODO: What has changed in the SQLA 2.0 tests that causes this to return an empty list?
        pass

    @skip("cockroachdb")  # noqa
    def test_metadata(self):
        # SQLA 2.0 test (indirectly) uses get_multi_columns(), so skip for now
        pass

    @skip("cockroachdb")  # noqa
    def test_not_existing_table(self):
        # TODO: Why "AssertionError: Callable did not raise an exception"?
        pass

    def test_unreflectable(self, connection):
        if connection.dialect.driver != "asyncpg":
            super().test_unreflectable(connection)


class ComponentReflectionTestExtra(_ComponentReflectionTestExtra):
    @testing.combinations(
        (True, testing.requires.schemas), (False,), argnames="use_schema"
    )
    @testing.requires.check_constraint_reflection
    def test_get_check_constraints(self, metadata, connection, use_schema):
        if connection.dialect.driver != "asyncpg":
            super().test_get_check_constraints(metadata, connection, use_schema, None)


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    def test_pk_column_order(self, connection):
        if config.db.driver != "asyncpg":
            super().test_pk_column_order(connection)


class HasIndexTest(_HasIndexTest):
    @skip("cockroachdb")  # noqa
    def test_has_index(self):
        """
        ObjectNotInPrerequisiteState: index "my_idx_2" in the middle of being added, try again later
        """  # noqa
        pass


class HasTableTest(_HasTableTest):
    @skip("cockroachdb")  # noqa
    def test_has_table_view(self):
        # has_table() kwarg 'info_cache' not supported here
        pass

    @skip("cockroachdb")  # noqa
    def test_has_table_cache(self):
        # has_table() kwarg 'info_cache' not supported here
        pass


class ExpandingBoundInTest(_ExpandingBoundInTest):
    @skip("cockroachdb")  # noqa
    def test_null_in_empty_set_is_false(self, connection):
        # Fixed in https://github.com/cockroachdb/cockroach/pull/49814
        # Unskip when backported.
        pass


class LongNameBlowoutTest(_LongNameBlowoutTest):
    @testing.combinations(
        ("fk",),
        ("pk",),
        ("ix",),
        ("ck"),  # (exclusion(s) omitted)
        ("uq"),  # (exclusion(s) omitted)
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        """ 
        https://github.com/cockroachdb/cockroach/issues/71908
        ... and also issues with ...
        "asyncpg.exceptions.InvalidParameterValueError: invalid locale C: language: tag is not well-formed"
        """  # noqa
        if connection.dialect.driver != "asyncpg":
            super().test_long_convention_name(type_, metadata, connection, None)


class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    def quote_fixtures(fn):
        return testing.combinations(
            ("quote ' one",),
            ('quote " two', testing.requires.symbol_names_w_double_quote),
        )(fn)

    @quote_fixtures
    def test_get_foreign_keys(self, name):
        if config.db.driver != "asyncpg":
            super().test_get_foreign_keys(name, None)

    @quote_fixtures
    def test_get_indexes(self, name):
        if config.db.driver != "asyncpg":
            super().test_get_indexes(name, None)

    @quote_fixtures
    def test_get_pk_constraint(self, name):
        if config.db.driver != "asyncpg":
            super().test_get_pk_constraint(name, None)


class NumericTest(_NumericTest):
    def test_float_as_decimal(self, do_numeric_test):
        # asyncpg.exceptions.DatatypeMismatchError: value type decimal doesn't match type
        # float4 of column "x"
        if config.db.dialect.driver != "asyncpg":  # noqa
            super().test_float_as_decimal(do_numeric_test)

    def test_float_as_float(self, do_numeric_test):
        # asyncpg.exceptions.DatatypeMismatchError: value type decimal doesn't match type
        # float4 of column "x"
        if config.db.dialect.driver != "asyncpg":  # noqa
            super().test_float_as_decimal(do_numeric_test)

    def test_float_custom_scale(self, do_numeric_test):
        # asyncpg.exceptions.DatatypeMismatchError: value type decimal doesn't match type
        # float4 of column "x"
        if config.db.dialect.driver != "asyncpg":  # noqa
            super().test_float_as_decimal(do_numeric_test)


class TrueDivTest(_TrueDivTest):
    @skip("cockroachdb")  # noqa
    def test_floordiv_integer(self):
        # we return SELECT 15 / 10 as Decimal('1.5'), not Integer
        pass

    @skip("cockroachdb")  # noqa
    def test_floordiv_integer_bound(self):
        # we return SELECT 15 / 10 as Decimal('1.5'), not Integer
        pass


class UnicodeSchemaTest(_UnicodeSchemaTest):
    def test_reflect(self, connection):
        # https://github.com/cockroachdb/cockroach/issues/71908
        if connection.dialect.driver != "asyncpg":
            super().test_reflect(connection)
