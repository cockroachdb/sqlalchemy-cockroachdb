from sqlalchemy import Table, Column, MetaData, select, testing
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer

from cockroachdb.sqlalchemy import run_transaction

meta = MetaData()

# Plain table object for the core test.
json_table = Table('json_model', meta,
                      Column('id', Integer, primary_key=True, autoincrement=False),
                      Column('data', JSONB))

# ORM class for the session test.
class JSONModel(declarative_base()):
    __table__ = json_table

class JSONTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        testing.db.execute(
                json_table.insert(),
                [dict(id=1, data={'a': 1}), dict(id=2, data={'b': 2})])

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def test_json(self):
        result = []
        query = select([json_table.c.data])
        for row in testing.db.execute(query):
            result.append(row.data)
        assert result == [{'a': 1}, {'b': 2}]

