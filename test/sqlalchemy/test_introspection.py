from sqlalchemy import Table, Column, MetaData, testing, ForeignKey, UniqueConstraint, \
    CheckConstraint
from sqlalchemy.types import Integer, String
from sqlalchemy.testing import fixtures

meta = MetaData()

customer_table = Table('customer', meta,
                       Column('id', Integer, primary_key=True),
                       Column('name', String),
                       Column('email', String),
                       UniqueConstraint('email'))

order_table = Table('order', meta,
                    Column('id', Integer, primary_key=True),
                    Column('customer_id', Integer, ForeignKey('customer.id')),
                    Column('info', String),
                    Column('status', String, CheckConstraint("status in ('open', 'closed')")))


class IntrospectionTest(fixtures.TestBase):
    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def setup_method(self):
        meta.create_all(testing.db)

    def test_create_metadata(self):
        # Create a metadata via introspection on the live DB.
        meta2 = MetaData(testing.db)

        # TODO(bdarnell): Do more testing.
        # For now just make sure it doesn't raise exceptions.
        # This covers get_foreign_keys(), which is apparently untested
        # in SQLAlchemy's dialect test suite.
        Table('customer', meta2, autoload=True)
        Table('order', meta2, autoload=True)
