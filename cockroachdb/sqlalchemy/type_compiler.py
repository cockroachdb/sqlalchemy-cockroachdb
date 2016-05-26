from sqlalchemy.dialects.postgresql.base import PGTypeCompiler


class TypeCompiler(PGTypeCompiler):
    def visit_TIMESTAMP(self, typ, **kw):
        # We don't yet parse the "WITH TIME ZONE" or "WITHOUT TIME
        # ZONE" modifiers that the superclass uses.
        return "TIMESTAMP"

    def visit_TIME(self, typ, **kw):
        return "TIME"
