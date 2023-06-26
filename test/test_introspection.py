import contextlib

from sqlalchemy import (
    Table,
    Column,
    MetaData,
    testing,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    text,
)
from sqlalchemy.types import Integer, String, Boolean
import sqlalchemy.types as sqltypes
from sqlalchemy.testing import fixtures
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import UUID

meta = MetaData()

customer_table = Table(
    "customer",
    meta,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("email", String),
    Column("verified", Boolean),
    UniqueConstraint("email"),
)

order_table = Table(
    "order",
    meta,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer, ForeignKey("customer.id")),
    Column("info", String),
    Column("status", String, CheckConstraint("status in ('open', 'closed')")),
)

# Regression test for https://github.com/cockroachdb/cockroach/issues/26993
index_table = Table("index", meta, Column("index", Integer, primary_key=True))
view_table = Table("view", meta, Column("view", Integer, primary_key=True))


class IntrospectionTest(fixtures.TestBase):
    __requires__ = ("sync_driver",)

    def teardown_method(self, method):
        meta.drop_all(testing.db)

    def setup_method(self):
        meta.create_all(testing.db)

    @testing.provide_metadata
    def test_create_metadata(self):
        # Create a metadata via introspection on the live DB.
        meta2 = self.metadata

        # TODO(bdarnell): Do more testing.
        # For now just make sure it doesn't raise exceptions.
        # This covers get_foreign_keys(), which is apparently untested
        # in SQLAlchemy's dialect test suite.
        Table("customer", meta2, autoload_with=testing.db)
        Table("order", meta2, autoload_with=testing.db)
        Table("index", meta2, autoload_with=testing.db)
        Table("view", meta2, autoload_with=testing.db)


class TestTypeReflection(fixtures.TestBase):
    __requires__ = ("sync_driver",)

    TABLE_NAME = "t"
    COLUMN_NAME = "c"

    @testing.provide_metadata
    def _test(self, typ, expected, array_item_type=None):
        with testing.db.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE {} ({} {})".format(
                        self.TABLE_NAME,
                        self.COLUMN_NAME,
                        typ,
                    )
                )
            )

        t = Table(self.TABLE_NAME, self.metadata, autoload_with=testing.db)
        c = t.c[self.COLUMN_NAME]
        assert isinstance(c.type, expected)
        if array_item_type:
            assert isinstance(c.type.item_type, array_item_type)

    def test_array(self):
        self._test("boolean[]", sqltypes.ARRAY, sqltypes.BOOLEAN)
        self._test("date[]", sqltypes.ARRAY, sqltypes.DATE)
        self._test("decimal[]", sqltypes.ARRAY, sqltypes.DECIMAL)
        self._test("float[]", sqltypes.ARRAY, sqltypes.FLOAT)
        self._test("int[]", sqltypes.ARRAY, sqltypes.INTEGER)
        self._test("timestamp[]", sqltypes.ARRAY, sqltypes.TIMESTAMP)
        self._test("varchar(10)[]", sqltypes.ARRAY, sqltypes.VARCHAR)

    def test_boolean(self):
        for t in ["bool", "boolean"]:
            self._test(t, sqltypes.BOOLEAN)

    def test_int(self):
        for t in ["bigint", "int", "int2", "int4", "int64", "int8", "integer", "smallint"]:
            self._test(t, sqltypes.INT)

    def test_float(self):
        for t in ["double precision", "float", "float4", "float8", "real"]:
            self._test(t, sqltypes.FLOAT)

    def test_decimal(self):
        for t in ["dec", "decimal", "numeric"]:
            self._test(t, sqltypes.DECIMAL)

    def test_date(self):
        self._test("date", sqltypes.DATE)

    def test_time(self):
        for t in ["time", "time without time zone"]:
            self._test(t, sqltypes.Time)

    def test_timestamp(self):
        types = [
            "timestamp",
            "timestamptz",
            "timestamp with time zone",
            "timestamp without time zone",
        ]
        for t in types:
            self._test(t, sqltypes.TIMESTAMP)

    def test_interval(self):
        self._test("interval", sqltypes.Interval)

    def test_varchar(self):
        types = [
            "char",
            "char varying",
            "character",
            "character varying",
            "string",
            "text",
            "varchar",
        ]
        for t in types:
            self._test(t, sqltypes.VARCHAR)

    def test_blob(self):
        for t in ["blob", "bytea", "bytes"]:
            self._test(t, sqltypes.BLOB)

    def test_json(self):
        for t in ["json", "jsonb"]:
            self._test(t, sqltypes.JSON)

    def test_uuid(self):
        self._test("uuid", UUID)

    def test_inet(self):
        self._test("inet", INET)


class UnknownTypeTest(fixtures.TestBase):
    __requires__ = ("sync_driver",)

    def setup_method(self):
        with testing.db.begin() as conn:
            conn.execute(text("CREATE TABLE t2 (c bool)"))

    def teardown_method(self):
        with testing.db.begin() as conn:
            conn.execute(text("DROP TABLE t2"))

    @testing.expect_warnings("Did not recognize type 'boolean'")
    def test_unknown_type(self):
        @contextlib.contextmanager
        def make_bool_unknown():
            import sqlalchemy_cockroachdb

            t = sqlalchemy_cockroachdb.base._type_map.pop("bool")
            sqlalchemy_cockroachdb.base._type_map.pop("boolean")
            yield
            sqlalchemy_cockroachdb.base._type_map["bool"] = t
            sqlalchemy_cockroachdb.base._type_map["boolean"] = t

        with make_bool_unknown():
            meta2 = MetaData()
            t = Table("t2", meta2, autoload_with=testing.db)
        assert t.c["c"].type == sqltypes.NULLTYPE
