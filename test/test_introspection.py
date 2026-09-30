from sqlalchemy import (
    Table,
    Column,
    MetaData,
    testing,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    text,
    inspect,
)
from sqlalchemy.types import Integer, String, Boolean
import sqlalchemy.types as sqltypes
from sqlalchemy.testing import fixtures
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql.base import PGDialect
from unittest import mock

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
        self._test("bytes[]", sqltypes.ARRAY, sqltypes.BLOB)
        self._test("date[]", sqltypes.ARRAY, sqltypes.DATE)
        self._test("decimal[]", sqltypes.ARRAY, sqltypes.DECIMAL)
        self._test("float[]", sqltypes.ARRAY, sqltypes.FLOAT)
        self._test("int[]", sqltypes.ARRAY, sqltypes.INTEGER)
        self._test("smallint[]", sqltypes.ARRAY, sqltypes.INTEGER)
        self._test("timestamp[]", sqltypes.ARRAY, sqltypes.TIMESTAMP)
        self._test("text[]", sqltypes.ARRAY, sqltypes.VARCHAR)
        self._test("varchar(10)[]", sqltypes.ARRAY, sqltypes.VARCHAR)

    def test_blob(self):
        for t in ["blob", "bytea", "bytes"]:
            self._test(t, sqltypes.BLOB)

    def test_boolean(self):
        for t in ["bool", "boolean"]:
            self._test(t, sqltypes.BOOLEAN)

    def test_char(self):
        for t in ["char", "character"]:
            self._test(t, sqltypes.CHAR)

    def test_date(self):
        self._test("date", sqltypes.DATE)

    def test_decimal(self):
        for t in ["dec", "decimal", "numeric"]:
            self._test(t, sqltypes.DECIMAL)

    def test_float(self):
        for t in ["double precision", "float", "float4", "float8", "real"]:
            self._test(t, sqltypes.FLOAT)

    def test_inet(self):
        self._test("inet", INET)

    def test_int(self):
        for t in ["bigint", "int", "int2", "int4", "int64", "int8", "integer", "smallint"]:
            self._test(t, sqltypes.INT)

    def test_interval(self):
        self._test("interval", INTERVAL)

    def test_json(self):
        for t in ["json", "jsonb"]:
            self._test(t, sqltypes.JSON)

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

    def test_uuid(self):
        self._test("uuid", UUID)

    def test_varchar(self):
        types = [
            "char varying",
            "character varying",
            "string",
            "text",
            "varchar",
        ]
        for t in types:
            self._test(t, sqltypes.VARCHAR)


class TableNamesTest(fixtures.TestBase):
    __requires__ = ("sync_driver",)

    def setup_method(self):
        with testing.db.begin() as conn:
            conn.execute(text("CREATE TABLE names_base (id INT PRIMARY KEY)"))
            conn.execute(text("CREATE VIEW names_view AS SELECT id FROM names_base"))
            conn.execute(
                text(
                    "CREATE TABLE names_ref (id INT PRIMARY KEY, "
                    "base_id INT REFERENCES names_base (id))"
                )
            )

    def teardown_method(self, method):
        with testing.db.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS names_ref"))
            conn.execute(text("DROP VIEW IF EXISTS names_view"))
            conn.execute(text("DROP TABLE IF EXISTS names_base"))

    def test_get_table_names_excludes_views(self):
        insp = inspect(testing.db)
        table_names = insp.get_table_names()
        assert "names_base" in table_names
        assert "names_view" not in table_names
        assert "names_view" in insp.get_view_names()

    def test_has_table_includes_views(self):
        insp = inspect(testing.db)
        assert insp.has_table("names_base")
        assert insp.has_table("names_view")
        assert not insp.has_table("names_absent")

    @testing.requires.schemas
    def test_get_table_names_uses_requested_schema(self):
        schema = testing.config.test_schema
        with testing.db.begin() as conn:
            conn.execute(text(f"CREATE TABLE {schema}.names_other (id INT PRIMARY KEY)"))
        try:
            insp = inspect(testing.db)
            table_names = insp.get_table_names(schema=schema)
            assert "names_other" in table_names
            assert "names_base" not in table_names
            assert "names_other" not in insp.get_table_names()
        finally:
            with testing.db.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.names_other"))

    def test_get_foreign_keys_passes_ignore_search_path(self):
        insp = inspect(testing.db)
        (fk,) = insp.get_foreign_keys("names_ref")
        assert fk["referred_table"] == "names_base"
        with mock.patch.object(
            PGDialect, "get_multi_foreign_keys", return_value={}
        ) as upstream:
            testing.db.dialect.get_multi_foreign_keys(
                None, None, None, None, None, postgresql_ignore_search_path=True
            )
        assert upstream.call_args.kwargs["postgresql_ignore_search_path"] is True
