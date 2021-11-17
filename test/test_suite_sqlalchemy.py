from sqlalchemy import __version__ as sa_version
from sqlalchemy.testing import combinations
from sqlalchemy.testing import skip
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import UnicodeSchemaTest as _UnicodeSchemaTest


class ComponentReflectionTest(_ComponentReflectionTest):
    def test_get_indexes(self, connection):
        if connection.dialect._is_v202plus and sa_version >= "1.4":
            super().test_get_indexes(connection, None, None)

    @combinations(
        ("noncol_idx_test_nopk", "noncol_idx_nopk"),
        ("noncol_idx_test_pk", "noncol_idx_pk"),
        argnames="tname,ixname",
    )
    def test_get_noncol_index(self, connection, tname, ixname):
        # https://github.com/cockroachdb/cockroach/issues/71908
        if connection.dialect.driver != "asyncpg":
            super().test_get_noncol_index(connection, tname, ixname)


class ExpandingBoundInTest(_ExpandingBoundInTest):
    @skip("cockroachdb")  # noqa
    def test_null_in_empty_set_is_false(self, connection):
        # Fixed in https://github.com/cockroachdb/cockroach/pull/49814
        # Unskip when backported.
        pass


class LongNameBlowoutTest(_LongNameBlowoutTest):
    @combinations(
        ("fk",),
        ("pk",),
        ("ix",),
        ("ck"),  # (exclusion(s) omitted)
        ("uq"),  # (exclusion(s) omitted)
        argnames="type_",
    )
    def test_long_convention_name(self, type_, metadata, connection):
        # https://github.com/cockroachdb/cockroach/issues/71908
        if not (connection.dialect.driver == "asyncpg" and type_ == "uq"):
            super().test_long_convention_name(type_, metadata, connection, None)


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


class UnicodeSchemaTest(_UnicodeSchemaTest):
    def test_reflect(self, connection):
        # https://github.com/cockroachdb/cockroach/issues/71908
        if connection.dialect.driver != "asyncpg":
            super().test_reflect(connection)
