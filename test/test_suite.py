import re

from sqlalchemy import text
from sqlalchemy.testing.suite import *  # noqa
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest


class ComponentReflectionTest(_ComponentReflectionTest):
    @testing.skip("cockroachdb")  # noqa
    def test_deprecated_get_primary_keys(self):
        # This is removed in the next sqlalchemy release (1.3.18)
        pass

    def test_get_indexes(self, connection):
        full_version_string = connection.execute(text("SELECT version()")).scalar()
        result = re.search(r" (v\d+\.\d+\.\d+) \(", full_version_string)
        if result:
            version_string = result.group(1)
            if version_string >= "v20.2":
                super().test_get_indexes(connection, None, None)


class ExpandingBoundInTest(_ExpandingBoundInTest):
    @testing.skip("cockroachdb")  # noqa
    def test_null_in_empty_set_is_false(self, connection):
        # Fixed in https://github.com/cockroachdb/cockroach/pull/49814
        # Unskip when backported.
        pass
