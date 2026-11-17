import re

from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.postgresql.asyncpg import PGExecutionContext_asyncpg
from .base import CockroachDBDialect
from .ddl_compiler import CockroachDDLCompiler
from .stmt_compiler import CockroachCompiler
from .stmt_compiler import CockroachIdentifierPreparer

# Errors CockroachDB raises when a schema change has made a prepared statement
# stale. asyncpg recognizes stale statements by PostgreSQL's routine names,
# which CockroachDB doesn't report, so these are matched by SQLSTATE and
# message instead.
_STALE_STATEMENT_ERRORS = (
    # The statement's result columns changed type.
    ("0A000", re.compile(r"cached plan must not change result type")),
    # A user-defined type the statement was prepared against was dropped.
    ("42704", re.compile(r"type with ID \d+ does not exist")),
    # A placeholder's type no longer fits its column (binds without casts,
    # e.g. textual SQL). Some fresh statements also raise this; harmless.
    ("42804", re.compile(r"placeholder \$\d+ already has type")),
)


class CockroachDBExecutionContext_asyncpg(PGExecutionContext_asyncpg):
    def handle_dbapi_exception(self, e):
        super().handle_dbapi_exception(e)
        sqlstate = getattr(e, "sqlstate", None)
        if any(
            sqlstate == code and message.search(str(e))
            for code, message in _STALE_STATEMENT_ERRORS
        ):
            self.dialect._invalidate_schema_cache()


class CockroachDBDialect_asyncpg(PGDialect_asyncpg, CockroachDBDialect):
    driver = "asyncpg"  # driver name
    preparer = CockroachIdentifierPreparer
    ddl_compiler = CockroachDDLCompiler
    statement_compiler = CockroachCompiler
    execution_ctx_cls = CockroachDBExecutionContext_asyncpg

    supports_statement_cache = True

    async def setup_asyncpg_json_codec(self, conn):
        # https://github.com/cockroachdb/cockroach/issues/9990#issuecomment-579202144
        pass

    def get_isolation_level_values(self, dbapi_conn):
        return ("SERIALIZABLE", "AUTOCOMMIT", "READ COMMITTED")
