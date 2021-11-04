from sqlalchemy import FLOAT, INTEGER, VARCHAR
from sqlalchemy.testing import skip
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import (
    BizarroCharacterFKResolutionTest as _BizarroCharacterFKResolutionTest,
)
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IsolationLevelTest as _IsolationLevelTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import TrueDivTest as _TrueDivTest
from sqlalchemy.testing.suite import UnicodeSchemaTest as _UnicodeSchemaTest


class BizarroCharacterFKResolutionTest(_BizarroCharacterFKResolutionTest):
    @testing.combinations(("id",), ("(3)",), ("col%p",), ("[brack]",), argnames="columnname")
    @testing.variation("use_composite", [True, False])
    @testing.combinations(
        ("plain",),
        ("(2)",),
        ("per % cent",),
        ("[brackets]",),
        argnames="tablename",
    )
    def test_fk_ref(self, connection, metadata, use_composite, tablename, columnname):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_fk_ref(connection, metadata, use_composite, tablename, columnname)


class ComponentReflectionTest(_ComponentReflectionTest):
    def test_get_indexes(self, connection):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_get_indexes(connection, None, None)

    @skip("cockroachdb")
    def test_get_noncol_index(self):
        # test not designed to handle ('desc', 'nulls_last')
        pass

    def test_get_multi_columns(self):
        insp = inspect(config.db)
        actual = insp.get_multi_columns()
        expected = {
            (None, "comment_test"): [
                {
                    "autoincrement": True,
                    "default": "unique_rowid()",
                    "is_hidden": False,
                    "name": "id",
                    "nullable": False,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "data",
                    "nullable": True,
                    "type": VARCHAR(length=20),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "d2",
                    "nullable": True,
                    "type": VARCHAR(length=20),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "d3",
                    "nullable": True,
                    "type": VARCHAR(length=42),
                },
            ],
            (None, "dingalings"): [
                {
                    "autoincrement": True,
                    "default": "unique_rowid()",
                    "is_hidden": False,
                    "name": "dingaling_id",
                    "nullable": False,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "address_id",
                    "nullable": True,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "id_user",
                    "nullable": True,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "data",
                    "nullable": True,
                    "type": VARCHAR(length=30),
                },
            ],
            (None, "email_addresses"): [
                {
                    "autoincrement": True,
                    "default": "unique_rowid()",
                    "is_hidden": False,
                    "name": "address_id",
                    "nullable": False,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "remote_user_id",
                    "nullable": True,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "email_address",
                    "nullable": True,
                    "type": VARCHAR(length=20),
                },
            ],
            (None, "no_constraints"): [
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "data",
                    "nullable": True,
                    "type": VARCHAR(length=20),
                }
            ],
            (None, "noncol_idx_test_nopk"): [
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "q",
                    "nullable": True,
                    "type": VARCHAR(length=5),
                }
            ],
            (None, "noncol_idx_test_pk"): [
                {
                    "autoincrement": True,
                    "default": "unique_rowid()",
                    "is_hidden": False,
                    "name": "id",
                    "nullable": False,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "q",
                    "nullable": True,
                    "type": VARCHAR(length=5),
                },
            ],
            (None, "users"): [
                {
                    "autoincrement": True,
                    "default": "unique_rowid()",
                    "is_hidden": False,
                    "name": "user_id",
                    "nullable": False,
                    "type": INTEGER(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "test1",
                    "nullable": False,
                    "type": VARCHAR(length=5),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "test2",
                    "nullable": False,
                    "type": FLOAT(),
                },
                {
                    "autoincrement": False,
                    "default": None,
                    "is_hidden": False,
                    "name": "parent_user_id",
                    "nullable": True,
                    "type": INTEGER(),
                },
            ],
        }
        eq_(len(actual), len(expected))
        eq_(actual.keys(), expected.keys())
        eq_(len(actual[(None, "comment_test")]), len(expected[(None, "comment_test")]))

    @skip("cockroachdb")
    def test_get_multi_indexes(self):
        pass

    @skip("cockroachdb")
    def test_get_multi_pk_constraint(self):
        pass

    @skip("cockroachdb")
    def test_get_pk_constraint(self):
        # we still have a "rowid" constraint when no explicit PK declared
        pass

    @skip("cockroachdb")
    def test_get_view_names(self):
        # TODO: What has changed in the SQLA 2.0 tests that causes this to return an empty list?
        pass

    @testing.combinations(True, False, argnames="use_schema")
    @testing.combinations(
        (True, testing.requires.views), False, argnames="views"
    )
    def test_metadata(self, connection, use_schema, views):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_metadata(connection, use_schema, views, [])

    @skip("cockroachdb")
    def test_not_existing_table(self):
        # TODO: Why "AssertionError: Callable did not raise an exception"?
        pass


class HasIndexTest(_HasIndexTest):
    @skip("cockroachdb")
    def test_has_index(self):
        """
        ObjectNotInPrerequisiteState: index "my_idx_2" in the middle of being added, try again later
        """
        pass


class HasTableTest(_HasTableTest):
    @skip("cockroachdb")
    def test_has_table_cache(self):
        pass


class InsertBehaviorTest(_InsertBehaviorTest):
    @skip("cockroachdb")
    def test_no_results_for_non_returning_insert(self):
        # we support RETURNING, so this should not be necessary
        pass


class IsolationLevelTest(_IsolationLevelTest):
    @skip("cockroachdb")
    def test_dialect_user_setting_is_restored(self):
        # IndexError: list index out of range
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
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_long_convention_name(type_, metadata, connection, None)


class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    def quote_fixtures(fn):
        return testing.combinations(
            ("quote ' one",),
            ('quote " two', testing.requires.symbol_names_w_double_quote),
        )(fn)

    @quote_fixtures
    def test_get_indexes(self, name):
        # could not decorrelate subquery
        if config.db.driver != "asyncpg":
            super().test_get_indexes(name, None)


class NumericTest(_NumericTest):
    def test_numeric_as_float(self, do_numeric_test):
        # psycopg.errors.InvalidParameterValue: unsupported binary operator: <decimal> + <float>
        if config.db.dialect.driver != "psycopg":
            super().test_numeric_as_float(do_numeric_test)

    def test_numeric_null_as_float(self, do_numeric_test):
        # psycopg.errors.InvalidParameterValue: unsupported binary operator: <decimal> + <float>
        if config.db.dialect.driver != "psycopg":
            super().test_numeric_null_as_float(do_numeric_test)


class TrueDivTest(_TrueDivTest):
    @skip("cockroachdb")
    def test_floordiv_integer(self):
        # we return SELECT 15 / 10 as Decimal('1.5'), not Integer
        pass

    @skip("cockroachdb")
    def test_floordiv_integer_bound(self):
        # we return SELECT 15 / 10 as Decimal('1.5'), not Integer
        pass


class UnicodeSchemaTest(_UnicodeSchemaTest):
    def test_reflect(self, connection):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_reflect(connection)
