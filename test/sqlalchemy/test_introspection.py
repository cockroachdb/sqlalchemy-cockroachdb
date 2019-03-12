from sqlalchemy import Table, Column, MetaData, testing, ForeignKey, UniqueConstraint, \
    CheckConstraint
from sqlalchemy.types import Integer, String, Boolean
import sqlalchemy.types as sqltypes
from sqlalchemy.testing import fixtures
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import UUID

meta = MetaData()

customer_table = Table('customer', meta,
                       Column('id', Integer, primary_key=True),
                       Column('name', String),
                       Column('email', String),
                       Column('verified', Boolean),
                       UniqueConstraint('email'))

order_table = Table('order', meta,
                    Column('id', Integer, primary_key=True),
                    Column('customer_id', Integer, ForeignKey('customer.id')),
                    Column('info', String),
                    Column('status', String, CheckConstraint("status in ('open', 'closed')")))

# Regression test for https://github.com/cockroachdb/cockroach/issues/26993
index_table = Table('index', meta,
                    Column('index', Integer, primary_key=True))
view_table = Table('view', meta,
                   Column('view', Integer, primary_key=True))


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
        Table('index', meta2, autoload=True)
        Table('view', meta2, autoload=True)


class TestTypeReflection(fixtures.TestBase):
    TABLE_NAME = 't'
    COLUMN_NAME = 'c'

    @testing.provide_metadata
    def _test(self, typ, expected):
        testing.db.execute(
            'CREATE TABLE {} ({} {})'.format(
                self.TABLE_NAME,
                self.COLUMN_NAME,
                typ,
            )
        )

        t = Table(self.TABLE_NAME, self.metadata, autoload=True)
        c = t.c[self.COLUMN_NAME]
        assert isinstance(c.type, expected)

    def test_boolean(self):
        for t in ['bool', 'boolean']:
            self._test(t, sqltypes.BOOLEAN)

    def test_int(self):
        for t in ['bigint', 'int', 'int2', 'int4', 'int64', 'int8', 'integer', 'smallint']:
            self._test(t, sqltypes.INT)

    def test_float(self):
        for t in ['double precision', 'float', 'float4', 'float8', 'real']:
            self._test(t, sqltypes.FLOAT)

    def test_decimal(self):
        for t in ['dec', 'decimal', 'numeric']:
            self._test(t, sqltypes.DECIMAL)

    def test_date(self):
        self._test('date', sqltypes.DATE)

    def test_time(self):
        for t in ['time', 'time without time zone']:
            self._test(t, sqltypes.Time)

    def test_timestamp(self):
        types = [
            'timestamp',
            'timestamptz',
            'timestamp with time zone',
            'timestamp without time zone',
        ]
        for t in types:
            self._test(t, sqltypes.TIMESTAMP)

    def test_interval(self):
        self._test('interval', sqltypes.Interval)

    def test_varchar(self):
        types = [
            'char',
            'char varying',
            'character',
            'character varying',
            'string',
            'text',
            'varchar',
        ]
        for t in types:
            self._test(t, sqltypes.VARCHAR)

    def test_blob(self):
        for t in ['blob', 'bytea', 'bytes']:
            self._test(t, sqltypes.BLOB)

    def test_json(self):
        for t in ['json', 'jsonb']:
            self._test(t, sqltypes.JSON)

    def test_uuid(self):
        self._test('uuid', UUID)

    def test_inet(self):
        self._test('inet', INET)
