from psycopg.crdb import connect as crdb_connect
from sqlalchemy import util
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg, PGDialectAsync_psycopg
from ._psycopg_common import _CockroachDBDialect_common_psycopg
from .ddl_compiler import CockroachDDLCompiler
from .stmt_compiler import CockroachCompiler
from .stmt_compiler import CockroachIdentifierPreparer


class CockroachDBDialect_psycopg(_CockroachDBDialect_common_psycopg, PGDialect_psycopg):
    driver = "psycopg"  # driver name
    preparer = CockroachIdentifierPreparer
    ddl_compiler = CockroachDDLCompiler
    statement_compiler = CockroachCompiler

    supports_statement_cache = True

    @util.memoized_property
    def _psycopg_json(self):
        from psycopg.types import json

        new_json = type("foo", (), {"Json": json.Jsonb, "Jsonb": json.Jsonb})
        return new_json

    def connect(
        self,
        disable_cockroachdb_telemetry=False,
        **kwargs,
    ):
        self.disable_cockroachdb_telemetry = util.asbool(disable_cockroachdb_telemetry)
        return crdb_connect(**kwargs)

    @classmethod
    def get_async_dialect_cls(cls, url):
        return CockroachDBDialectAsync_psycopg


class CockroachDBDialectAsync_psycopg(_CockroachDBDialect_common_psycopg, PGDialectAsync_psycopg):
    is_async = True
    supports_statement_cache = True


dialect = CockroachDBDialect_psycopg
dialect_async = CockroachDBDialectAsync_psycopg
