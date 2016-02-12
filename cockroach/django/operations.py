from django.db.backends.postgresql.operations import DatabaseOperations as PostgresDatabaseOperations

from django.utils import timezone


class DatabaseOperations(PostgresDatabaseOperations):
    def adapt_datetimefield_value(self, value):
        # Cribbed from django.db.backends.mysql.operations
        if value is None:
            return None
        # CockroachDB doesn't support TZ-aware datetimes.
        if timezone.is_aware(value):
            value = timezone.make_naive(value, self.connection.timezone)
        return value
