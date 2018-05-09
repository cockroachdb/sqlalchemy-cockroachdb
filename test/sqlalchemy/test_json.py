from sqlalchemy import Table, Column, MetaData, select, testing
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer

from sqlalchemy import JSON as BaseJSON

meta = MetaData()

# Plain table object for the core test.
# Try all three JSON type objects.
json_table = Table('json_model', meta,
                   Column('id', Integer, primary_key=True, autoincrement=False),
                   Column('jsonb_data', JSONB),
                   Column('json_data', JSON),
                   Column('base_json_data', BaseJSON))


# ORM class for the session test.
class JSONModel(declarative_base()):
    __table__ = json_table


class JSONTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        testing.db.execute(
            json_table.insert(),
            [dict(id=1,
                  jsonb_data={'a': 1},
                  json_data={'b': 2},
                  base_json_data={'c': 3}),
             dict(id=2,
                  jsonb_data={'d': 4},
                  json_data={'e': 5},
                  base_json_data={'f': 6})])

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def test_json(self):
        result = []
        query = select([json_table.c.jsonb_data,
                        json_table.c.json_data,
                        json_table.c.base_json_data,
        ])
        for row in testing.db.execute(query):
            result.append((row.jsonb_data, row.json_data, row.base_json_data))
        assert result == [({'a': 1}, {'b': 2}, {'c': 3}),
                          ({'d': 4}, {'e': 5}, {'f': 6})]


class JSONSessionTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        self.sessionmaker = sessionmaker(testing.db)
        session = self.sessionmaker()
        session.add(JSONModel(id=1,
                              jsonb_data={'a': 1},
                              json_data={'b': 2},
                              base_json_data={'c': 3}))
        session.add(JSONModel(id=2,
                              jsonb_data={'d': 4},
                              json_data={'e': 5},
                              base_json_data={'f': 6}))
        session.commit()

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def test_json(self):
        session = self.sessionmaker()
        result = []
        for row in session.query(JSONModel).all():
            result.append((row.jsonb_data, row.json_data, row.base_json_data))
        assert result == [({'a': 1}, {'b': 2}, {'c': 3}),
                          ({'d': 4}, {'e': 5}, {'f': 6})]
