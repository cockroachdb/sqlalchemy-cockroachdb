from sqlalchemy import Table, Column, select, testing
from sqlalchemy.dialects.postgresql import JSONB, JSON
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import fixtures, eq_
from sqlalchemy.types import Integer

from sqlalchemy import JSON as BaseJSON


class JSONTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "json_model",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("jsonb_data", JSONB),
            Column("json_data", JSON),
            Column("base_json_data", BaseJSON),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.json_model.insert(),
            [
                dict(id=1, jsonb_data={"a": 1}, json_data={"b": 2}, base_json_data={"c": 3}),
                dict(id=2, jsonb_data={"d": 4}, json_data={"e": 5}, base_json_data={"f": 6}),
            ],
        )

    def test_json(self, connection):
        if not testing.db.dialect._has_native_json:
            return
        json_table = self.tables.json_model
        result = []
        query = select(json_table.c.jsonb_data, json_table.c.json_data, json_table.c.base_json_data)
        for row in connection.execute(query):
            result.append((row.jsonb_data, row.json_data, row.base_json_data))
        eq_(result, [({"a": 1}, {"b": 2}, {"c": 3}), ({"d": 4}, {"e": 5}, {"f": 6})])


class JSONSessionTest(fixtures.TestBase):
    __backend__ = True

    def _fixture(self):
        Base = declarative_base()

        class JSONModel(Base):
            __tablename__ = "json_model"
            id = Column(Integer, primary_key=True, autoincrement=False)
            jsonb_data = Column(JSONB)
            json_data = Column(JSON)
            base_json_data = Column(BaseJSON)

        return JSONModel

    def test_json(self, connection):
        if not testing.db.dialect._has_native_json:
            return

        JSONModel = self._fixture()
        meta = JSONModel.metadata
        meta.create_all(connection)

        Session = sessionmaker(connection)
        session = Session()
        try:
            session.add_all(
                [
                    JSONModel(
                        id=1, jsonb_data={"a": 1}, json_data={"b": 2}, base_json_data={"c": 3}
                    ),
                    JSONModel(
                        id=2, jsonb_data={"d": 4}, json_data={"e": 5}, base_json_data={"f": 6}
                    ),
                ]
            )
            session.commit()
            result = []
            for row in session.query(JSONModel).all():
                result.append((row.jsonb_data, row.json_data, row.base_json_data))
            eq_(result, [({"a": 1}, {"b": 2}, {"c": 3}), ({"d": 4}, {"e": 5}, {"f": 6})])
        finally:
            meta.drop_all(connection)
