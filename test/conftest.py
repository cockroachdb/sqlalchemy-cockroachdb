from sqlalchemy.dialects import registry
import pytest

registry.register(
    "cockroachdb",
    "sqlalchemy_cockroachdb.psycopg2",
    "CockroachDBDialect_psycopg2",
)
registry.register(
    "cockroachdb.psycopg2",
    "sqlalchemy_cockroachdb.psycopg2",
    "CockroachDBDialect_psycopg2",
)

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
