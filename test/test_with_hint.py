from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import provide_metadata


class WithHintTest(fixtures.TestBase, AssertsCompiledSQL):
    @provide_metadata
    def test_with_hint(self):
        meta = self.metadata
        t = Table(
            "t",
            meta,
            Column("id", Integer),
            Column("txt", String(50)),
            Index("ix_t_txt", "txt"),
        )
        self.assert_compile(
            select(t).with_hint(t, "ix_t_txt"),
            "SELECT t.id, t.txt FROM t@ix_t_txt",
        )
        if config.db.dialect.driver == "psycopg2":
            param_placeholder = "%(id_1)s"
            cast_str = ""
        elif config.db.dialect.driver == "asyncpg":
            param_placeholder = "$1"
            cast_str = "::INTEGER"
        elif config.db.dialect.driver == "psycopg":
            param_placeholder = "%(id_1)s"
            cast_str = "::INTEGER"
        self.assert_compile(
            select(t).with_hint(t, "ix_t_txt").where(t.c.id < 3),
            f"SELECT t.id, t.txt FROM t@ix_t_txt WHERE t.id < {param_placeholder}{cast_str}",
        )
