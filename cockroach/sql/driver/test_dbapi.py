import dbapi20
import time
import unittest

import cockroach.sql.driver


class DBAPITest(dbapi20.DatabaseAPI20Test):
    def setUp(self):
        self.driver = cockroach.sql.driver
        # Generate unique database names because the test doesn't
        # always clean up after itself.
        # TODO(bdarnell): improve cleanup so we don't need this.
        database = time.strftime("test%y%m%d%H%M%S")
        self.connect_kw_args = dict(addr="localhost:26257",
                                    user="root",
                                    database=database,
                                    auto_create=True)
        # Patch the table creation statements because we require primary keys.
        self.ddl1 = self._patch_create_table(self.ddl1)
        self.ddl2 = self._patch_create_table(self.ddl2)
        super(DBAPITest, self).setUp()

    def _patch_create_table(self, ddl):
        if ddl[-1] != ")":
            raise Exception("Expected ddl to end in ')': %s", ddl)
        return ddl[:-1] + ", primary key (name))"

    # The following features are optional and we do not implement them.
    # For some reason the test suite has placeholder tests for these
    # features that just raise NotImplementedError, so we must override
    # and disable them.
    def test_nextset(self):
        raise unittest.SkipTest("nextset is not implemented")

    def test_setoutputsize(self):
        raise unittest.SkipTest("setoutputsize is not implemented")
