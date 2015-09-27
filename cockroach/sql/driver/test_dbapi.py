import dbapi20
import unittest

import cockroach.sql.driver


class DBAPITest(dbapi20.DatabaseAPI20Test):
    def setUp(self):
        self.driver = cockroach.sql.driver
        super(DBAPITest, self).setUp()

    def test_nextset(self):
        raise unittest.SkipTest("TODO(bdarnell)")

    def test_setoutputsize(self):
        raise unittest.SkipTest("TODO(bdarnell)")
