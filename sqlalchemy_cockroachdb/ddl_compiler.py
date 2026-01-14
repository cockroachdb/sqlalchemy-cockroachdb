from sqlalchemy import exc
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from collections.abc import Sequence
from typing import Any, cast

from sqlalchemy import ColumnElement, exc
from sqlalchemy.ext.compiler import compiles  # type: ignore[import-untyped]
from sqlalchemy.schema import CreateIndex, CreateTable, Index, Table
from sqlalchemy.sql import coercions, expression, roles
from sqlalchemy.sql.compiler import DDLCompiler  # type: ignore[import-untyped]
from sqlalchemy_cockroachdb.base import (  # type: ignore[import-untyped]
    CockroachDBDialect,
)
from sqlalchemy_cockroachdb.ddl_compiler import (  # type: ignore[import-untyped]
    CockroachDDLCompiler,
)


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


# TODO: convert visitors to memeber functions on CockroachDDLCompiler (it's like
# this now just because I wrote+tested it inside our codebase).


@compiles(CreateTable, "cockroachdb")
def visit_create_table(
    element: CreateTable, compiler: CockroachDDLCompiler, **kw: Any
) -> str:
    out = compiler.visit_create_table(element, **kw)

    assert isinstance(element.target, Table)

    if len(element.target.indexes) > 0:
        indexes = [
            _codegen_index(i, compiler, include_schema=False, **kw)
            for i in element.target.indexes
        ]

        # TODO: Not compatible with anything that uses post_create_table, we
        # need to parse properly to find the `)` which matches `CREATE TABLE (`.
        out = out.rstrip().rstrip(")").rstrip()
        out += ",\n"
        out += ",\n\t".join(indexes)
        out += "\n)"

        # Record that we created these indexes so that we can double check it
        # later.
        for index in element.target.indexes:
            index.info["_cockroachdb_index_created_by_create_table"] = True

    return out


@compiles(CreateIndex, "cockroachdb")
def visit_create_index(element: Any, compiler: CockroachDDLCompiler, **kw: Any) -> str:
    index = element.target
    assert isinstance(index, Index)
    was_created = index.info.get("_cockroachdb_index_created_by_create_table", False)
    assert was_created

    return "SELECT 'No-op: in cockroachdb we put index creation DDL inside the corresponding CREATE TABLE for improved performance.'"


# Copy+paste of private function DDLCompiler._prepared_index_name
def _prepared_index_name(
    index: Index, compiler: DDLCompiler, include_schema: bool = False
) -> str:
    if index.table is not None:
        effective_schema = compiler.preparer.schema_for_object(index.table)
    else:
        effective_schema = None
    if include_schema and effective_schema:
        schema_name = compiler.preparer.quote_schema(effective_schema)
    else:
        schema_name = None

    index_name: str = cast(str, compiler.preparer.format_index(index))

    if schema_name:
        index_name = schema_name + "." + index_name
    return index_name


IDX_USING = re.compile(r"^(?:btree|hash|gist|gin|[\w_]+)$", re.I)


# Heavily based on DDLCompiler.visit_create_index
def _codegen_index(
    index: Index, compiler: DDLCompiler, include_schema: bool, **kw: Any
) -> str:
    # I think this is only nullable before _set_parent is called. We shouldn't
    # need to emit DDL for any indexes in that state.
    assert index.table is not None

    text = ""

    # TODO: check this more carefully, I'm winging it here. Do all supported
    # postgres USINGs map to INVERTED? Why didn't we need this before these changes?
    using = index.dialect_options["postgresql"]["using"]
    if using:
        assert using.lower() in ("gin", "gist")
        text += "INVERTED "

    if index.unique:
        text += "UNIQUE "
        assert not using

    # I don't think we strictly need an index name, but best to require one for
    # sqlalchemy compat with any other database.
    if index.name is None:
        raise exc.CompileError("CREATE INDEX requires that the index have a name")

    text += "INDEX %s " % _prepared_index_name(
        index, compiler, include_schema=include_schema
    )

    ops = index.dialect_options["postgresql"]["ops"]
    text += "(%s)" % (
        ", ".join(
            [
                compiler.sql_compiler.process(
                    (
                        expr.self_group()
                        if not isinstance(expr, expression.ColumnClause)
                        else expr
                    ),
                    include_table=False,
                    literal_binds=True,
                )
                + (
                    (" " + ops[expr.key])
                    if hasattr(expr, "key") and expr.key in ops
                    else ""
                )
                for expr in cast(Sequence[ColumnElement[Any]], index.expressions)
            ]
        )
    )

    includeclause = index.dialect_options["postgresql"]["include"]
    if includeclause:
        inclusions = [
            index.table.c[col] if isinstance(col, str) else col for col in includeclause
        ]
        text += " INCLUDE (%s)" % ", ".join(
            [compiler.preparer.quote(c.name) for c in inclusions]
        )

    # TODO: I don't think crdb supports this feature?
    # nulls_not_distinct = index.dialect_options["postgresql"]["nulls_not_distinct"]
    # if nulls_not_distinct is True:
    #     text += " NULLS NOT DISTINCT"
    # elif nulls_not_distinct is False:
    #     text += " NULLS DISTINCT"

    withclause = index.dialect_options["postgresql"]["with"]
    if withclause:
        text += " WITH (%s)" % (
            ", ".join(
                [
                    "%s = %s" % storage_parameter
                    for storage_parameter in withclause.items()
                ]
            )
        )

    # TODO: I don't think crdb supports this feature?
    # tablespace_name = index.dialect_options["postgresql"]["tablespace"]
    # if tablespace_name:
    #     text += " TABLESPACE %s" % compiler.preparer.quote(tablespace_name)

    whereclause = index.dialect_options["postgresql"]["where"]
    if whereclause is not None:
        whereclause = coercions.expect(roles.DDLExpressionRole, whereclause)

        where_compiled = compiler.sql_compiler.process(
            whereclause, include_table=False, literal_binds=True
        )
        text += " WHERE " + where_compiled

    return text
