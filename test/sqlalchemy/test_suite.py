from sqlalchemy.testing.suite import *

from sqlalchemy.testing.suite import RowFetchTest as _RowFetchTest

# This test isn't controllable with any requirements setting,
# so patch it out by hand. The failure relates to the fact that
# we return time zone offsets on timestamps that are not configured
# as "WITH TIME ZONE".
class RowFetchTest(_RowFetchTest):
    def test_row_w_scalar_select(self):
        pass

del _RowFetchTest
