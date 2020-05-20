import pytest

from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import (
    ComponentReflectionTest as _ComponentReflectionTest
)
from sqlalchemy.testing.suite import (
    ExpandingBoundInTest as _ExpandingBoundInTest
)


class ComponentReflectionTest(_ComponentReflectionTest):
    @pytest.mark.skip()
    def test_deprecated_get_primary_keys(self):
        # This is removed in the next sqlalchemy release (1.3.18)
        pass


class ExpandingBoundInTest(_ExpandingBoundInTest):
    @pytest.mark.skip()
    def test_null_in_empty_set_is_false(self, connection):
        # Fixed in https://github.com/cockroachdb/cockroach/pull/49814
        # Unskip when backported.
        pass
