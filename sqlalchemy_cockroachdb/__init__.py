from sqlalchemy.dialects import registry as _registry

__version__ = "1.3.4.dev0"

_registry.register(
    "cockroachdb.psycopg2",
    "sqlalchemy_cockroachdb.psycopg2",
    "CockroachDBDialect_psycopg2",
)
