from alembic.testing.suite import *  # noqa
from sqlalchemy.testing import skip
from alembic.testing.suite import AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest
from alembic.testing.suite import AutogenerateForeignKeysTest as _AutogenerateForeignKeysTest
from alembic.testing.suite import BackendAlterColumnTest as _BackendAlterColumnTest
from alembic.testing.suite import IncludeHooksTest as _IncludeHooksTest


class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    def test_change_ondelete_from_restrict(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_change_ondelete_from_restrict()

    def test_change_onupdate_from_restrict(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_change_onupdate_from_restrict()

    def test_nochange_ondelete(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_nochange_ondelete()

    def test_nochange_ondelete_noaction(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_nochange_ondelete_noaction()

    def test_nochange_ondelete_restrict(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_nochange_ondelete_restrict()

    def test_nochange_onupdate(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_nochange_onupdate()

    def test_nochange_onupdate_noaction(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_nochange_onupdate_noaction()

    def test_nochange_onupdate_restrict(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_nochange_onupdate_restrict()


class AutogenerateForeignKeysTest(_AutogenerateForeignKeysTest):
    def test_casing_convention_changed_so_put_drops_first(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_casing_convention_changed_so_put_drops_first()

    def test_no_change(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_no_change()

    def test_no_change_colkeys(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_no_change_colkeys()

    def test_no_change_composite_fk(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_no_change_composite_fk()

    def test_remove_composite_fk(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_remove_composite_fk()

    def test_remove_fk(self):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_remove_fk()


class BackendAlterColumnTest(_BackendAlterColumnTest):
    @skip("cockroachdb")  # noqa
    def test_modify_nullable_to_non(self):
        # previously needed "with self.op.get_context().autocommit_block():"
        # which is no longer valid in SQLA 2.0
        pass

    @skip("cockroachdb")  # noqa
    def test_modify_type_int_str(self):
        # TODO: enable this test when warning removed for ALTER COLUMN int → string
        pass


class IncludeHooksTest(_IncludeHooksTest):
    @combinations(("object",), ("name",))  # noqa
    def test_change_fk(self, hook_type):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_change_fk(hook_type)

    @combinations(("object",), ("name",))  # noqa
    def test_remove_connection_fk(self, hook_type):
        if config.db.dialect._is_v202plus:  # noqa
            super().test_remove_connection_fk(hook_type)
