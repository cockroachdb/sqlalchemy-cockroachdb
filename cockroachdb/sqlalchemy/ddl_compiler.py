from sqlalchemy.dialects.postgresql.base import PGDDLCompiler


class DDLCompiler(PGDDLCompiler):
    def visit_foreign_key_constraint(self, constraint):
        # Drop all foreign key constraints. We support them now but at
        # least one test fails with them enabled because we don't
        # support dropping tables with self-referential foreign keys
        # (#12916)
        return None
