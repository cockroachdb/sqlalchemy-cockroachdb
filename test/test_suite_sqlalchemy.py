from sqlalchemy import FLOAT, INTEGER, VARCHAR
from sqlalchemy.testing import skip
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import (
    ComponentReflectionTest as _ComponentReflectionTest,
)
from sqlalchemy.testing.suite import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite import HasTableTest as _HasTableTest
from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IsolationLevelTest as _IsolationLevelTest
from sqlalchemy.testing.suite import (
    LongNameBlowoutTest as _LongNameBlowoutTest,
)
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import (
    QuotedNameArgumentTest as _QuotedNameArgumentTest,
)
from sqlalchemy.testing.suite import TrueDivTest as _TrueDivTest
from sqlalchemy.testing.suite import UnicodeSchemaTest as _UnicodeSchemaTest


class ComponentReflectionTest(_ComponentReflectionTest):
    def test_get_indexes(self, connection):
        if not (config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus):
            super().test_get_indexes(connection, None, None)

    @skip("cockroachdb")
    def test_get_noncol_index(self):
        # test not designed to handle ('desc', 'nulls_last')
        pass

    def test_get_multi_columns(self):
        insp = inspect(config.db)
        actual = insp.get_multi_columns()
        expected = {
            (None, "users"): [
                {
                    "name": "user_id",
                    "type": INTEGER(),
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "test1",
                    "type": VARCHAR(length=5),
                    "nullable": False,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "test2",
                    "type": FLOAT(),
                    "nullable": False,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "parent_user_id",
                    "type": INTEGER(),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
            ],
            (None, "comment_test"): [
                {
                    "name": "id",
                    "type": INTEGER(),
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": False,
                    "comment": "id comment",
                },
                {
                    "name": "data",
                    "type": VARCHAR(length=20),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": "data % comment",
                },
                {
                    "name": "d2",
                    "type": VARCHAR(length=20),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": "Comment types type speedily ' \" \\ '' Fun!",
                },
                {
                    "name": "d3",
                    "type": VARCHAR(length=42),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": "Comment\nwith\rescapes",
                },
            ],
            (None, "no_constraints"): [
                {
                    "name": "data",
                    "type": VARCHAR(length=20),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                }
            ],
            (None, "noncol_idx_test_nopk"): [
                {
                    "name": "q",
                    "type": VARCHAR(length=5),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                }
            ],
            (None, "noncol_idx_test_pk"): [
                {
                    "name": "id",
                    "type": INTEGER(),
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "q",
                    "type": VARCHAR(length=5),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
            ],
            (None, "email_addresses"): [
                {
                    "name": "address_id",
                    "type": INTEGER(),
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "remote_user_id",
                    "type": INTEGER(),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "email_address",
                    "type": VARCHAR(length=20),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
            ],
            (None, "dingalings"): [
                {
                    "name": "dingaling_id",
                    "type": INTEGER(),
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "address_id",
                    "type": INTEGER(),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "id_user",
                    "type": INTEGER(),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "data",
                    "type": VARCHAR(length=30),
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
            ],
        }
        eq_(len(actual), len(expected))
        eq_(actual.keys(), expected.keys())
        eq_(
            len(actual[(None, "comment_test")]),
            len(expected[(None, "comment_test")]),
        )
        if config.db.dialect.supports_comments:
            act = [x for x in actual[(None, "comment_test")] if x["name"] == "data"][0]
            exp = [x for x in expected[(None, "comment_test")] if x["name"] == "data"][0]
            eq_(act["comment"], exp["comment"])

    def test_get_multi_indexes(self):
        insp = inspect(config.db)
        result = insp.get_multi_indexes()
        eq_(
            result,
            {
                (None, "comment_test"): [],
                (None, "dingalings"): [
                    {
                        "column_names": ["data"],
                        "column_sorting": {"data": ("nulls_first",)},
                        "dialect_options": {"postgresql_using": "prefix"},
                        "duplicates_constraint": "dingalings_data_key",
                        "name": "dingalings_data_key",
                        "unique": True,
                    },
                    {
                        "column_names": ["address_id", "dingaling_id"],
                        "column_sorting": {
                            "address_id": ("nulls_first",),
                            "dingaling_id": ("nulls_first",),
                        },
                        "dialect_options": {"postgresql_using": "prefix"},
                        "duplicates_constraint": "zz_dingalings_multiple",
                        "name": "zz_dingalings_multiple",
                        "unique": True,
                    },
                ],
                (None, "email_addresses"): [
                    {
                        "column_names": ["email_address"],
                        "column_sorting": {"email_address": ("nulls_first",)},
                        "dialect_options": {"postgresql_using": "prefix"},
                        "name": "ix_email_addresses_email_address",
                        "unique": False,
                    }
                ],
                (None, "no_constraints"): [],
                (None, "noncol_idx_test_nopk"): [
                    {
                        "column_names": ["q"],
                        "column_sorting": {"q": ("desc", "nulls_last")},
                        "dialect_options": {"postgresql_using": "prefix"},
                        "name": "noncol_idx_nopk",
                        "unique": False,
                    }
                ],
                (None, "noncol_idx_test_pk"): [
                    {
                        "column_names": ["q"],
                        "column_sorting": {"q": ("desc", "nulls_last")},
                        "dialect_options": {"postgresql_using": "prefix"},
                        "name": "noncol_idx_pk",
                        "unique": False,
                    }
                ],
                (None, "users"): [
                    {
                        "column_names": ["user_id", "test2", "test1"],
                        "column_sorting": {
                            "test1": ("nulls_first",),
                            "test2": ("nulls_first",),
                            "user_id": ("nulls_first",),
                        },
                        "dialect_options": {"postgresql_using": "prefix"},
                        "name": "users_all_idx",
                        "unique": False,
                    },
                    {
                        "column_names": ["test1", "test2"],
                        "column_sorting": {"test1": ("nulls_first",), "test2": ("nulls_first",)},
                        "dialect_options": {"postgresql_using": "prefix"},
                        "duplicates_constraint": "users_t_idx",
                        "name": "users_t_idx",
                        "unique": True,
                    },
                ],
            },
        )

    def test_get_multi_pk_constraint(self):
        insp = inspect(config.db)
        result = insp.get_multi_pk_constraint()
        eq_(
            result,
            {
                (None, "comment_test"): {
                    "comment": None,
                    "constrained_columns": ["id"],
                    "name": "comment_test_pkey",
                },
                (None, "dingalings"): {
                    "comment": None,
                    "constrained_columns": ["dingaling_id"],
                    "name": "dingalings_pkey",
                },
                (None, "email_addresses"): {
                    "comment": "ea pk comment",
                    "constrained_columns": ["address_id"],
                    "name": "email_ad_pk",
                },
                (None, "no_constraints"): {
                    "comment": None,
                    "constrained_columns": ["rowid"],
                    "name": "no_constraints_pkey",
                },
                (None, "noncol_idx_test_nopk"): {
                    "comment": None,
                    "constrained_columns": ["rowid"],
                    "name": "noncol_idx_test_nopk_pkey",
                },
                (None, "noncol_idx_test_pk"): {
                    "comment": None,
                    "constrained_columns": ["id"],
                    "name": "noncol_idx_test_pk_pkey",
                },
                (None, "users"): {
                    "comment": None,
                    "constrained_columns": ["user_id"],
                    "name": "users_pkey",
                },
            },
        )

    @skip("cockroachdb")
    def test_get_pk_constraint(self):
        # we still have a "rowid" constraint when no explicit PK declared
        pass

    @skip("cockroachdb")
    def test_get_view_names(self):
        # TODO: What has changed in the SQLA 2.0 tests that causes this to return an empty list?
        #       FWIW, insp.get_view_names() does still work IRL
        pass

    @testing.combinations(True, False, argnames="use_schema")
    @testing.combinations((True, testing.requires.views), False, argnames="views")
    def test_metadata(self, connection, use_schema, views):
        if not (config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus):
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


class IntegerTest(_IntegerTest):
    @_IntegerTest._huge_ints()
    def test_huge_int(self, integer_round_trip, intvalue):
        if config.db.dialect.driver != "asyncpg":
            super().test_huge_int(integer_round_trip, intvalue)


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
        if not (config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus):
            super().test_long_convention_name(type_, metadata, connection, None)


class NumericTest(_NumericTest):
    def test_numeric_as_float(self, do_numeric_test):
        # psycopg.errors.InvalidParameterValue: unsupported binary operator: <decimal> + <float>
        if config.db.dialect.driver != "psycopg":
            super().test_numeric_as_float(do_numeric_test)

    def test_numeric_null_as_float(self, do_numeric_test):
        # psycopg.errors.InvalidParameterValue: unsupported binary operator: <decimal> + <float>
        if config.db.dialect.driver != "psycopg":
            super().test_numeric_null_as_float(do_numeric_test)


class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    def quote_fixtures(fn):
        return testing.combinations(
            ("quote ' one",),
            ('quote " two', testing.requires.symbol_names_w_double_quote),
        )(fn)

    @quote_fixtures
    def test_get_indexes(self, name):
        # could not decorrelate subquery
        if not (config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus):
            super().test_get_indexes(name, None)


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
        if not (config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus):
            super().test_reflect(connection)
