from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.postgresql.schema import DatabaseSchemaEditor as PostgresDatabaseSchemaEditor


class DatabaseSchemaEditor(PostgresDatabaseSchemaEditor):
    def _model_indexes_sql(self, model):
        # Postgres customizes _model_indexes_sql to add special-case
        # options for string fields. Skip to the base class version
        # to avoid this.
        return BaseDatabaseSchemaEditor._model_indexes_sql(self, model)
