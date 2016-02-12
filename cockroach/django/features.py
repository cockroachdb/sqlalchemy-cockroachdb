from django.db.backends.postgresql.features import DatabaseFeatures as PostgresDatabaseFeatures


class DatabaseFeatures(PostgresDatabaseFeatures):
    supports_timezones = False
    supports_foreign_keys = False
    supports_column_check_constraints = False
