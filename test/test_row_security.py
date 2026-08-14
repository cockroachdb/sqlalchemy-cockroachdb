from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.dialects.postgresql import CreatePolicy
from sqlalchemy.dialects.postgresql import EnableRowLevelSecurity
from sqlalchemy.dialects.postgresql import ForceRowLevelSecurity
from sqlalchemy.dialects.postgresql import Policy
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import expect_raises_message


class RowSecurityCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "cockroachdb"

    def test_inherits_postgresql_policy_compiler(self):
        table = Table(
            "item",
            MetaData(),
            Column("owner_id", Integer),
            schema="app",
        )
        policy = Policy(
            "read",
            table,
            command="SELECT",
            using=table.c.owner_id == 7,
        )

        self.assert_compile(
            CreatePolicy(policy),
            "CREATE POLICY read ON app.item FOR SELECT TO PUBLIC " "USING (owner_id = 7)",
            literal_binds=True,
        )

    def test_rejects_unsupported_current_role(self):
        table = Table("item", MetaData(), Column("owner_id", Integer))
        policy = Policy("read", table, roles=("CURRENT_ROLE",))

        with expect_raises_message(
            exc.CompileError,
            "CockroachDB row security does not support CURRENT_ROLE",
        ):
            self.assert_compile(CreatePolicy(policy), "")


class RowSecurityReflectionTest(fixtures.TestBase):
    __only_on__ = "cockroachdb"
    __requires__ = ("sync_driver",)

    @testing.provide_metadata
    def test_postgresql_compatible_reflection(self):
        table = Table(
            "row_security_reflection",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("owner_id", Integer),
        )
        with testing.db.begin() as connection:
            table.create(connection)
            connection.execute(EnableRowLevelSecurity(table))
            connection.execute(ForceRowLevelSecurity(table))
            connection.execute(
                CreatePolicy(
                    Policy(
                        "read",
                        table,
                        command="SELECT",
                        using=table.c.owner_id == 7,
                    )
                )
            )

            state = inspect(connection).get_row_security(table.name)

        eq_(
            state,
            {
                "enabled": True,
                "forced": True,
                "policies": [
                    {
                        "name": "read",
                        "command": "SELECT",
                        "roles": ["public"],
                        "using": "owner_id = 7:::INT8",
                        "check": None,
                        "permissive": True,
                    }
                ],
            },
        )
