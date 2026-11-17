from sqlalchemy import exc, text
from sqlalchemy.testing import engines, eq_, expect_raises_message, fixtures


class CachedPlanInvalidationTest(fixtures.TestBase):
    """A prepared statement made stale by DDL from another process fails once,
    and then the dialect re-prepares it."""

    __only_on__ = "cockroachdb+asyncpg"
    __backend__ = True

    def _drop_all(self, conn):
        conn.execute(text("DROP TABLE IF EXISTS cached_plan_t"))
        conn.execute(text("DROP TYPE IF EXISTS cached_plan_e"))

    def _ddl(self, *statements):
        with engines.testing_engine().begin() as conn:
            for statement in statements:
                conn.execute(text(statement))

    def setup_test(self):
        with engines.testing_engine().begin() as conn:
            self._drop_all(conn)

    def teardown_test(self):
        with engines.testing_engine().begin() as conn:
            self._drop_all(conn)

    def _assert_recovers(self, query, params, ddl, message):
        # DDL emitted through the same dialect invalidates its caches up front,
        # so the schema change has to come from a separate engine.
        with engines.testing_engine().connect() as conn:
            conn.execute(query, params)
            conn.commit()

            self._ddl(*ddl)

            with expect_raises_message(exc.DBAPIError, message):
                conn.execute(query, params)
            conn.rollback()

            result = conn.execute(query, params)
            return result.all() if result.returns_rows else None

    def test_result_type_change(self):
        self._ddl(
            "CREATE TABLE cached_plan_t (id INT PRIMARY KEY, a STRING)",
            "INSERT INTO cached_plan_t VALUES (1, 'x')",
        )
        rows = self._assert_recovers(
            text("SELECT * FROM cached_plan_t"),
            {},
            ["ALTER TABLE cached_plan_t ADD COLUMN b STRING"],
            # asyncpg versions that classify this error replace its message.
            "cached plan must not change result type|cached statement plan is invalid",
        )
        eq_(rows, [(1, "x", None)])

    def test_type_recreated(self):
        create = [
            "CREATE TYPE cached_plan_e AS ENUM ('a', 'b')",
            "CREATE TABLE cached_plan_t (id INT PRIMARY KEY, v cached_plan_e)",
        ]
        self._ddl(*create)
        self._assert_recovers(
            text("UPSERT INTO cached_plan_t (id, v) VALUES (:id, CAST(:v AS cached_plan_e))"),
            {"id": 1, "v": "a"},
            ["DROP TABLE cached_plan_t", "DROP TYPE cached_plan_e", *create],
            "type with ID",
        )
