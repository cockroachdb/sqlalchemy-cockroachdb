from .base import CockroachDBDialect


class _CockroachDBDialect_common_psycopg(CockroachDBDialect):
    supports_sane_rowcount = False  # for psycopg2, at least

    def get_isolation_level_values(self, dbapi_conn):
        return ("SERIALIZABLE", "AUTOCOMMIT")
