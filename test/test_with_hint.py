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
    __requires__ = ("sync_driver",)

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
        param_placeholder = "%s" if config.db.dialect.is_async else "%(id_1)s"
        self.assert_compile(
            select(t).with_hint(t, "ix_t_txt").where(t.c.id < 3),
            f"SELECT t.id, t.txt FROM t@ix_t_txt WHERE t.id < {param_placeholder}",
        )
