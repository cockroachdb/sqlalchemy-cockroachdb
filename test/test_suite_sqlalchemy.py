from sqlalchemy import __version__ as sa_version
from sqlalchemy.testing import skip
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest


class ComponentReflectionTest(_ComponentReflectionTest):
    def test_get_indexes(self, connection):
        if connection.dialect._is_v202plus and sa_version >= "1.4":
            super().test_get_indexes(connection, None, None)


class ExpandingBoundInTest(_ExpandingBoundInTest):
    @skip("cockroachdb")  # noqa
    def test_null_in_empty_set_is_false(self, connection):
        # Fixed in https://github.com/cockroachdb/cockroach/pull/49814
        # Unskip when backported.
        pass
