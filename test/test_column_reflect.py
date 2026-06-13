from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    testing,
    inspect,
    BigInteger,
    Identity,
    Computed,
)
from sqlalchemy.testing import fixtures, eq_, config

meta = MetaData()

with_pk = Table(
    "with_pk",
    meta,
    Column("id", Integer, primary_key=True),
    Column("txt", String),
)

with_pk_other_schema = Table(
    "with_pk_other_schema",
    meta,
    Column("id", Integer, primary_key=True),
    Column("txt", String),
    schema="test_schema",
)

without_pk = Table(
    "without_pk",
    meta,
    Column("txt", String),
)

with_identity = Table(
    "with_identity",
    meta,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("txt", String),
)

with_identity_always = Table(
    "with_identity_always",
    meta,
    Column("id", BigInteger, Identity(always=True), primary_key=True),
    Column("txt", String),
)

with_computed_stored = Table(
    "with_computed_stored",
    meta,
    Column("id", Integer, primary_key=True),
    Column("id2", Integer, Computed("id + 1", persisted=True)),
)

# Note: CockroachDB computed columns do not support 'virtual' persistence;
#       set the 'persisted' flag to None or True for CockroachDB support.


class ReflectSpecialColumnsTest(fixtures.TestBase):
    __requires__ = ("sync_driver",)

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def setup_method(self):
        meta.create_all(testing.db)

    def _get_col_info(self, table_name, schema=None, include_hidden=False):
        insp = inspect(testing.db)
        col_info = insp.get_columns(table_name, schema, include_hidden=include_hidden)
        for row in col_info:
            row["type"] = str(row["type"])
        return col_info

    def test_reflect_hidden_columns(self):
        eq_(
            self._get_col_info("with_pk"),
            [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "txt",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
            ],
        )

        eq_(
            self._get_col_info("without_pk"),  # include_hidden=False
            [
                {
                    "name": "txt",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
            ],
        )

        eq_(
            self._get_col_info("without_pk", include_hidden=True),
            [
                {
                    "name": "txt",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default": None,
                    "autoincrement": False,
                    "is_hidden": False,
                    "comment": None,
                },
                {
                    "name": "rowid",
                    "type": "INTEGER",
                    "nullable": False,
                    "default": "unique_rowid()",
                    "autoincrement": True,
                    "is_hidden": True,
                    "comment": None,
                },
            ],
        )

    def test_two_hidden(self):
        with testing.db.begin() as conn:
            conn.exec_driver_sql(
                "create table tmp ("
                "id int primary key,"
                "hidden1 int not visible,"
                "hidden2 int not visible,"
                "int_col int"
                ")"
            )

        eq_(
            self._get_col_info("tmp", include_hidden=False),
            [
                {
                    "autoincrement": False,
                    "comment": None,
                    "default": None,
                    "is_hidden": False,
                    "name": "id",
                    "nullable": False,
                    "type": "INTEGER",
                },
                {
                    "autoincrement": False,
                    "comment": None,
                    "default": None,
                    "is_hidden": False,
                    "name": "int_col",
                    "nullable": True,
                    "type": "INTEGER",
                },
            ],
        )

        with testing.db.begin() as conn:
            conn.exec_driver_sql("drop table tmp")

    def test_reflect_identity(self):
        if config.db.dialect._is_v263plus:
            eq_(
                self._get_col_info("with_identity"),
                [
                    {
                        "autoincrement": True,
                        "comment": None,
                        "default": None,
                        "identity": {
                            "always": False,
                            "cache": 1,
                            "cycle": False,
                            "increment": 1,
                            "maxvalue": 9223372036854775807,
                            "minvalue": 1,
                            "start": 1,
                        },
                        "is_hidden": False,
                        "name": "id",
                        "nullable": False,
                        "type": "INTEGER",
                    },
                    {
                        "autoincrement": False,
                        "comment": None,
                        "default": None,
                        "is_hidden": False,
                        "name": "txt",
                        "nullable": True,
                        "type": "VARCHAR",
                    },
                ],
            )

            eq_(
                self._get_col_info("with_identity_always"),
                [
                    {
                        "autoincrement": True,
                        "comment": None,
                        "default": None,
                        "identity": {
                            "always": True,
                            "cache": 1,
                            "cycle": False,
                            "increment": 1,
                            "maxvalue": 9223372036854775807,
                            "minvalue": 1,
                            "start": 1,
                        },
                        "is_hidden": False,
                        "name": "id",
                        "nullable": False,
                        "type": "INTEGER",
                    },
                    {
                        "autoincrement": False,
                        "comment": None,
                        "default": None,
                        "is_hidden": False,
                        "name": "txt",
                        "nullable": True,
                        "type": "VARCHAR",
                    },
                ],
            )

    def test_reflect_computed_stored(self):
        eq_(
            self._get_col_info("with_computed_stored"),
            [
                {
                    "autoincrement": True,
                    "comment": None,
                    "default": "unique_rowid()",
                    "is_hidden": False,
                    "name": "id",
                    "nullable": False,
                    "type": "INTEGER",
                },
                {
                    "autoincrement": False,
                    "comment": None,
                    "computed": {"persisted": True, "sqltext": "id + 1"},
                    "default": None,
                    "is_hidden": False,
                    "name": "id2",
                    "nullable": True,
                    "type": "INTEGER",
                },
            ],
        )

    def test_reflect_other_schema(self):
        # verify https://github.com/cockroachdb/cockroach/issues/170049
        if config.db.dialect._is_v263plus:
            eq_(
                self._get_col_info("with_pk_other_schema", schema="test_schema"),
                [
                    {
                        "autoincrement": True,
                        "comment": None,
                        "default": "unique_rowid()",
                        "is_hidden": False,
                        "name": "id",
                        "nullable": False,
                        "type": "INTEGER",
                    },
                    {
                        "autoincrement": False,
                        "comment": None,
                        "default": None,
                        "is_hidden": False,
                        "name": "txt",
                        "nullable": True,
                        "type": "VARCHAR",
                    },
                ],
            )
