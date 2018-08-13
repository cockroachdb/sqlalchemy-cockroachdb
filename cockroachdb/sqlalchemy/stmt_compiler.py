from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer
from sqlalchemy.dialects.postgresql.psycopg2 import PGCompiler_psycopg2

# This is extracted from CockroachDB's `sql.y`. Add keywords here if *NEW* reserved keywords
# are added to sql.y. DO NOT DELETE keywords here, even if they are deleted from sql.y:
# once a keyword in CockroachDB, forever a keyword in clients (because of cross-version compat).
crdb_grammar_reserved = """
  ALL
| ANALYSE
| ANALYZE
| AND
| ANY
| ARRAY
| AS
| ASC
| ASYMMETRIC
| BOTH
| CASE
| CAST
| CHECK
| COLLATE
| COLUMN
| CONSTRAINT
| CREATE
| CURRENT_CATALOG
| CURRENT_DATE
| CURRENT_ROLE
| CURRENT_SCHEMA
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| DEFAULT
| DEFERRABLE
| DESC
| DISTINCT
| DO
| ELSE
| END
| EXCEPT
| FALSE
| FETCH
| FOR
| FOREIGN
| FROM
| GRANT
| GROUP
| HAVING
| IN
| INDEX
| INITIALLY
| INTERSECT
| INTO
| LATERAL
| LEADING
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| NOT
| NOTHING
| NULL
| OFFSET
| ON
| ONLY
| OR
| ORDER
| PLACING
| PRIMARY
| REFERENCES
| RETURNING
| ROLE
| SELECT
| SESSION_USER
| SOME
| SYMMETRIC
| TABLE
| THEN
| TO
| TRAILING
| TRUE
| UNION
| UNIQUE
| USER
| USING
| VARIADIC
| VIEW
| VIRTUAL
| WHEN
| WHERE
| WINDOW
| WITH
| WORK
"""
CRDB_RESERVED_WORDS = set([x.strip().lower() for x in crdb_grammar_reserved.split('|')])


class CockroachIdentifierPreparer(PGIdentifierPreparer):
    reserved_words = CRDB_RESERVED_WORDS


class CockroachCompiler(PGCompiler_psycopg2):
    pass
