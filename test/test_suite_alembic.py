from alembic.testing.suite import *  # noqa
from sqlalchemy.testing import skip
from alembic.testing.suite import AutogenerateFKOptionsTest as _AutogenerateFKOptionsTest
from alembic.testing.suite import BackendAlterColumnTest as _BackendAlterColumnTest


class AutogenerateFKOptionsTest(_AutogenerateFKOptionsTest):
    @skip("cockroachdb")
    def test_nochange_ondelete(self):
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
