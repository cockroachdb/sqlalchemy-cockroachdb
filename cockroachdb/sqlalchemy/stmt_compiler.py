from sqlalchemy.dialects.postgresql.psycopg2 import PGCompiler_psycopg2, expression


class CockroachCompiler(PGCompiler_psycopg2):

    # TODO(bdarnell): remove this when cockroachdb/cockroach#17008 is fixed.
    def returning_clause(self, stmt, returning_cols):
        columns = [
            '.'.join(self._label_select_column(None, c, True, False, {}).split('.')[-2:])
            for c in expression._select_iterables(returning_cols)
        ]

        return 'RETURNING ' + ', '.join(columns)
