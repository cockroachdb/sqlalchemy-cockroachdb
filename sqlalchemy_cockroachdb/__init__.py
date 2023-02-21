from sqlalchemy.dialects import registry as _registry
from .transaction import run_transaction  # noqa

__version__ = "2.0.0"

_registry.register(
    "cockroachdb.psycopg2",
    "sqlalchemy_cockroachdb.psycopg2",
    "CockroachDBDialect_psycopg2",
)
_registry.register(
    "cockroachdb.asyncpg",
    "sqlalchemy_cockroachdb.asyncpg",
    "CockroachDBDialect_asyncpg",
)
