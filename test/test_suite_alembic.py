from alembic.testing.suite import *  # noqa
from sqlalchemy.testing import skip
from alembic.testing.suite import AutogenerateComputedTest as _AutogenerateComputedTest
from alembic.testing.suite import AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest
from alembic.testing.suite import AutogenerateForeignKeysTest as _AutogenerateForeignKeysTest
from alembic.testing.suite import AutoincrementTest as _AutoincrementTest
from alembic.testing.suite import BackendAlterColumnTest as _BackendAlterColumnTest
from alembic.testing.suite import IncludeHooksTest as _IncludeHooksTest


class AutogenerateComputedTest(_AutogenerateComputedTest):
    def test_add_computed_column(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_add_computed_column()

    @testing.combinations(
        lambda: (None, sa.Computed("bar*5")),
        (lambda: (sa.Computed("bar*5"), None)),
        lambda: (
            sa.Computed("bar*5"),
            sa.Computed("bar * 42", persisted=True),
        ),
        lambda: (sa.Computed("bar*5"), sa.Computed("bar * 42")),
    )
    @config.requirements.computed_reflects_normally
    def test_cant_change_computed_warning(self, test_case):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_cant_change_computed_warning(test_case)

    @testing.combinations(
        lambda: (None, None),
        lambda: (sa.Computed("5"), sa.Computed("5")),
        lambda: (sa.Computed("bar*5"), sa.Computed("bar*5")),
        (
            lambda: (sa.Computed("bar*5"), None),
            config.requirements.computed_doesnt_reflect_as_server_default,
        ),
    )
    def test_computed_unchanged(self, test_case):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_computed_unchanged(test_case, [])

    def test_remove_computed_column(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_remove_computed_column()


class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    def test_change_ondelete_from_restrict(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_change_ondelete_from_restrict()

    def test_change_onupdate_from_restrict(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_change_onupdate_from_restrict()

    def test_nochange_ondelete(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_nochange_ondelete()

    def test_nochange_ondelete_noaction(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_nochange_ondelete_noaction()

    def test_nochange_ondelete_restrict(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_nochange_ondelete_restrict()

    def test_nochange_onupdate(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_nochange_onupdate()

    def test_nochange_onupdate_noaction(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_nochange_onupdate_noaction()

    def test_nochange_onupdate_restrict(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_nochange_onupdate_restrict()


class AutogenerateForeignKeysTest(_AutogenerateForeignKeysTest):
    def test_add_composite_fk_with_name(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_add_composite_fk_with_name()

    def test_add_fk(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_add_fk()

    def test_add_fk_colkeys(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_add_fk_colkeys()

    def test_casing_convention_changed_so_put_drops_first(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_casing_convention_changed_so_put_drops_first()

    def test_no_change(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_no_change()

    def test_no_change_colkeys(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_no_change_colkeys()

    def test_no_change_composite_fk(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_no_change_composite_fk()

    def test_remove_composite_fk(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_remove_composite_fk()

    def test_remove_fk(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_remove_fk()


class AutoincrementTest(_AutoincrementTest):
    def test_alter_column_autoincrement_compositepk_explicit_true(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_compositepk_explicit_true()

    def test_alter_column_autoincrement_compositepk_false(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_compositepk_false()

    def test_alter_column_autoincrement_compositepk_implicit_false(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_compositepk_implicit_false()

    def test_alter_column_autoincrement_none(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_none()

    def test_alter_column_autoincrement_nonpk_explicit_true(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_nonpk_explicit_true()

    def test_alter_column_autoincrement_nonpk_false(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_nonpk_false()

    def test_alter_column_autoincrement_nonpk_implicit_false(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_nonpk_implicit_false()

    def test_alter_column_autoincrement_pk_explicit_true(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_pk_explicit_true()

    def test_alter_column_autoincrement_pk_false(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_pk_false()

    def test_alter_column_autoincrement_pk_implicit_true(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_alter_column_autoincrement_pk_implicit_true()


class BackendAlterColumnTest(_BackendAlterColumnTest):
    @skip("cockroachdb")
    def test_modify_nullable_to_non(self):
        # previously needed "with self.op.get_context().autocommit_block():"
        # which is no longer valid in SQLA 2.0
        pass

    @skip("cockroachdb")
    def test_modify_type_int_str(self):
        # TODO: enable this test when warning removed for ALTER COLUMN int → string
        pass


class IncludeHooksTest(_IncludeHooksTest):
    def test_add_metadata_fk(self):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_add_metadata_fk()

    @combinations(("object",), ("name",))
    @config.requirements.no_name_normalize
    def test_change_fk(self, hook_type):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_change_fk(hook_type)

    @combinations(("object",), ("name",))
    @config.requirements.no_name_normalize
    def test_remove_connection_fk(self, hook_type):
        if not (
            config.db.dialect.driver == "asyncpg" and not config.db.dialect._is_v231plus
        ):
            super().test_remove_connection_fk(hook_type)
