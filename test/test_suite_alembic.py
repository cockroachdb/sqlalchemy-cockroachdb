from alembic.testing.suite import *  # noqa
# from sqlalchemy.testing import skip
from alembic.testing.suite import BackendAlterColumnTest as _BackendAlterColumnTest


class BackendAlterColumnTest(_BackendAlterColumnTest):
    def test_modify_nullable_to_non(self):
        if config.db.dialect._is_v253plus:
            super().test_modify_nullable_to_non()

    def test_modify_type_int_str(self):
        if config.db.dialect._is_v253plus:
            super().test_modify_type_int_str()
