from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from .base import CockroachDBDialect
from .ddl_compiler import CockroachDDLCompiler
from .stmt_compiler import CockroachCompiler
from .stmt_compiler import CockroachIdentifierPreparer


class CockroachDBDialect_asyncpg(PGDialect_asyncpg, CockroachDBDialect):
    driver = "asyncpg"  # driver name
    preparer = CockroachIdentifierPreparer
    ddl_compiler = CockroachDDLCompiler
    statement_compiler = CockroachCompiler

    supports_statement_cache = True

    async def setup_asyncpg_json_codec(self, conn):
        # https://github.com/cockroachdb/cockroach/issues/9990#issuecomment-579202144
        pass
