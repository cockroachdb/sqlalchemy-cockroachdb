from sqlalchemy import MetaData, Table, Column, Integer, String, testing, inspect
from sqlalchemy.testing import fixtures, eq_

meta = MetaData()

with_pk = Table(
    "with_pk",
    meta,
    Column("id", Integer, primary_key=True),
    Column("txt", String),
)

without_pk = Table(
    "without_pk",
    meta,
    Column("txt", String),
)


class ReflectHiddenColumnsTest(fixtures.TestBase):
    __requires__ = ("sync_driver",)

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def setup_method(self):
        meta.create_all(testing.db)

    def _get_col_info(self, table_name, include_hidden=False):
        insp = inspect(testing.db)
        col_info = insp.get_columns(table_name, include_hidden=include_hidden)
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
