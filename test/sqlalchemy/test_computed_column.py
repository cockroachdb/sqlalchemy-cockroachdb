from sqlalchemy import Table, Column, Computed, MetaData, select, testing
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer

meta = MetaData()

# Plain table for the core test.
computed_table = Table('computed', meta,
                       Column('id', Integer, primary_key=True, autoincrement=False),
                       Column('computed_id', Integer, Computed('id * id')))


# ORM class for the session test.
class ComputedModel(declarative_base()):
    __table__ = computed_table


class ComputedColumnTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        testing.db.execute(computed_table.insert(),
                           [dict(id=2), dict(id=3)])

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def test_computed_column(self):
        result = []
        query = select([computed_table.c.id,
                        computed_table.c.computed_id])
        for row in testing.db.execute(query):
            result.append((row.id, row.computed_id))
        assert result == [(2, 4), (3, 9)]


class JSONSessionTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        self.sessionmaker = sessionmaker(testing.db)
        session = self.sessionmaker()
        session.add(ComputedModel(id=2))
        session.add(ComputedModel(id=3))
        session.commit()

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def test_json(self):
        session = self.sessionmaker()
        result = []
        for row in session.query(ComputedModel).all():
            result.append((row.id, row.computed_id))
        assert result == [(2, 4), (3, 9)]
