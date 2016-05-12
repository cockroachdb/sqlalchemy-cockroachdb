import sqlalchemy.sql.compiler
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler


class DDLCompiler(PGDDLCompiler):
    def get_column_specification(self, column, **kwargs):
        # Same as superclass version but replaces SERIAL with
        # unique_rowid.
        colspec = self.preparer.format_column(column)
        default = None
        if column.primary_key and \
           column is column.table._autoincrement_column:
            colspec += " INTEGER"
            default = "unique_rowid()"
        else:
            colspec += " " + self.dialect.type_compiler.process(
                column.type, type_expression=column)
            default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_foreign_key_constraint(self, constraint):
        # Drop all foreign key constraints because we don't support
        # them yet.
        return None
