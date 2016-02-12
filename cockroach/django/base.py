from django.db.backends.postgresql.base import DatabaseWrapper as PostgresDatabaseWrapper
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor


class DatabaseWrapper(PostgresDatabaseWrapper):
    vendor = 'cockroachdb'

    # Override some types from the postgresql adapter.
    data_types = dict(PostgresDatabaseWrapper.data_types,
                      AutoField='integer',
                      DateTimeField='timestamp')
    data_types_suffix = dict(PostgresDatabaseWrapper.data_types_suffix,
                             AutoField='DEFAULT experimental_unique_int()')
    # Disable checks for positive values on some fields.
    data_type_check_constraints = {}

    SchemaEditorClass = DatabaseSchemaEditor

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.features = DatabaseFeatures(self)
        self.introspection = DatabaseIntrospection(self)
        self.ops = DatabaseOperations(self)
