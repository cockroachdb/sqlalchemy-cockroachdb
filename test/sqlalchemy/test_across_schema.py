from sqlalchemy import Table, MetaData, testing
from sqlalchemy.testing import fixtures


class AcrossSchemaTest(fixtures.TestBase):
    TEST_DATABASE = 'test_sqlalchemy_across_schema'

    def teardown_method(self, method):
        testing.db.execute("DROP TABLE IF EXISTS {}.users".format(self.TEST_DATABASE))
        testing.db.execute("DROP DATABASE IF EXISTS {}".format(self.TEST_DATABASE))

    def setup_method(self):
        testing.db.execute("CREATE DATABASE IF NOT EXISTS {}".format(self.TEST_DATABASE))
        testing.db.execute("CREATE TABLE IF NOT EXISTS {}.users (name STRING PRIMARY KEY)".format(self.TEST_DATABASE))
        self.meta = MetaData(testing.db, schema=self.TEST_DATABASE)

    def test_get_columns_indexes_across_schema(self):
        # get_columns and get_indexes use default db uri schema.
        # across schema table must use schema.table
        Table('users', self.meta, autoload=True, schema=self.TEST_DATABASE)

    def test_returning_clause(self):
        # TODO(bdarnell): remove this when cockroachdb/cockroach#17008 is fixed.
        # across schema returning is schema.table.id but cockroachdb not support.
        table = Table('users', self.meta, autoload=True, schema=self.TEST_DATABASE)
        table.insert().values(dict(name='John')).returning(table.c.name).execute()
