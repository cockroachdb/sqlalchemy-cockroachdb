from sqlalchemy.dialects.postgresql.base import PGTypeCompiler


class TypeCompiler(PGTypeCompiler):
    def visit_TIMESTAMP(self, typ, **kw):
        # We don't yet parse the "WITH TIME ZONE" or "WITHOUT TIME
        # ZONE" modifiers that the superclass uses.
        return "TIMESTAMP"

    def visit_TIME(self, typ, **kw):
        return "TIME"

    def visit_DECIMAL(self, type_, **kw):
        # Map DECIMAL to FLOAT (which is something sqlalchemy supports
        # for databases that don't have a native decimal type.
        # We do, but the tests all fail if we try to use it. It looks
        # like the tests rely on either implicit casts or type inference
        # for literals.
        #
        # TODO(bdarnell): replace with DECIMAL when it works, perhaps
        # once the new type system is in.
        return "FLOAT"

    visit_NUMERIC = visit_DECIMAL
