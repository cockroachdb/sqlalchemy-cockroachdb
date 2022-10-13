from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from ._psycopg_common import _CockroachDBDialect_common_psycopg
from .ddl_compiler import CockroachDDLCompiler
from .stmt_compiler import CockroachCompiler
from .stmt_compiler import CockroachIdentifierPreparer


class CockroachDBDialect_psycopg2(_CockroachDBDialect_common_psycopg, PGDialect_psycopg2):
    driver = "psycopg2"  # driver name
    preparer = CockroachIdentifierPreparer
    ddl_compiler = CockroachDDLCompiler
    statement_compiler = CockroachCompiler

    supports_statement_cache = True
