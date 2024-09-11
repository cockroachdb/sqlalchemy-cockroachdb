from alembic.testing.suite import *  # noqa
from sqlalchemy.testing import skip
from alembic.testing.suite import AutogenerateCommentsTest as _AutogenerateCommentsTest
from alembic.testing.suite import AutogenerateComputedTest as _AutogenerateComputedTest
from alembic.testing.suite import AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest
from alembic.testing.suite import AutogenerateForeignKeysTest as _AutogenerateForeignKeysTest
from alembic.testing.suite import BackendAlterColumnTest as _BackendAlterColumnTest
from alembic.testing.suite import IncludeHooksTest as _IncludeHooksTest


class AutogenerateCommentsTest(_AutogenerateCommentsTest):
    @skip("cockroachdb")
    def test_add_column_comment(self):
        pass

    @skip("cockroachdb")
    def test_add_table_comment(self):
        pass

    @skip("cockroachdb")
    def test_alter_column_comment(self):
        pass

    @skip("cockroachdb")
    def test_alter_table_comment(self):
        pass

    @skip("cockroachdb")
    def test_existing_column_comment_no_change(self):
        pass

    @skip("cockroachdb")
    def test_existing_table_comment_no_change(self):
        pass

    @skip("cockroachdb")
    def test_remove_column_comment(self):
        pass

    @skip("cockroachdb")
    def test_remove_table_comment(self):
        pass


class AutogenerateComputedTest(_AutogenerateComputedTest):
    @skip("cockroachdb")
    def test_add_computed_column(self):
        pass

    @skip("cockroachdb")
    def test_cant_change_computed_warning(self):
        pass

    @skip("cockroachdb")
    def test_computed_unchanged(self):
        pass

    @skip("cockroachdb")
    def test_remove_computed_column(self):
        pass


class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    @skip("cockroachdb")
    def test_nochange_ondelete(self):
        pass


class AutogenerateForeignKeysTest(_AutogenerateForeignKeysTest):
    @skip("cockroachdb")
    def test_add_composite_fk_with_name(self):
        pass

    @skip("cockroachdb")
    def test_add_fk(self):
        pass

    @skip("cockroachdb")
    def test_add_fk_colkeys(self):
        pass

    @skip("cockroachdb")
    def test_casing_convention_changed_so_put_drops_first(self):
        pass

    @skip("cockroachdb")
    def test_no_change(self):
        pass

    @skip("cockroachdb")
    def test_no_change_colkeys(self):
        pass

    @skip("cockroachdb")
    def test_no_change_composite_fk(self):
        pass

    @skip("cockroachdb")
    def test_remove_composite_fk(self):
        pass

    @skip("cockroachdb")
    def test_remove_fk(self):
        pass


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
    @skip("cockroachdb")
    def test_add_metadata_fk(self):
        pass

    @skip("cockroachdb")
    def test_change_fk(self):
        pass

    @skip("cockroachdb")
    def test_remove_connection_fk(self):
        pass
