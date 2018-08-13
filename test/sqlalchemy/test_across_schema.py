from sqlalchemy import Table, MetaData, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy import distinct


class AcrossSchemaTest(fixtures.TestBase):
    def teardown_method(self, method):
        if not testing.db.dialect._is_v2plus:
            return
        testing.db.execute("DROP TABLE IF EXISTS users")

    def setup_method(self):
        if not testing.db.dialect._is_v2plus:
            return

        testing.db.execute("""CREATE TABLE IF NOT EXISTS users (
                                  name STRING PRIMARY KEY
                              )
                           """)
        self.meta = MetaData(testing.db, schema='public')

    def test_get_columns_indexes_across_schema(self):
        if not testing.db.dialect._is_v2plus:
            return

        # get_columns and get_indexes use default db uri schema.
        # across schema table must use schema.table
        Table('users', self.meta, autoload=True, schema='public')
        Table('columns', self.meta, autoload=True, schema='information_schema')

    def test_returning_clause(self):
        if not testing.db.dialect._is_v2plus:
            return

        # TODO(bdarnell): remove this when cockroachdb/cockroach#17008 is fixed.
        # across schema returning is schema.table.id but cockroachdb not support.
        table = Table('users', self.meta, autoload=True, schema='public')
        table.insert().values(dict(name='John')).returning(table.c.name).execute()

    def test_using_info_schema(self):
        if not testing.db.dialect._is_v2plus:
            return

        table = Table('columns', self.meta, autoload=True, schema='information_schema')
        sm = sessionmaker(testing.db)
        session = sm()
        assert session.query(func.count(distinct(table.columns['table_name']))).scalar() > 1
