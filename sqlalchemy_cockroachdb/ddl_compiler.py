from sqlalchemy import exc
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler


class CockroachDDLCompiler(PGDDLCompiler):
    def visit_computed_column(self, generated, **kw):
        if generated.persisted is False:
            raise exc.CompileError(
                "CockroachDB computed columns do not support 'virtual' "
                "persistence; set the 'persisted' flag to None or True for "
                "CockroachDB support."
            )

        return "AS (%s) STORED" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
