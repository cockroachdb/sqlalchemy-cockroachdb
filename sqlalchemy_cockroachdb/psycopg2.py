from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from .base import CockroachDBDialect
from .ddl_compiler import CockroachDDLCompiler
from .stmt_compiler import CockroachIdentifierPreparer


class CockroachDBDialect_psycopg2(PGDialect_psycopg2, CockroachDBDialect):
    name = "cockroachdb"  # dialect name
    driver = "psycopg2"  # driver name
    preparer = CockroachIdentifierPreparer
    ddl_compiler = CockroachDDLCompiler

    supports_statement_cache = True
