from sqlalchemy.testing.provision import temp_table_keyword_args
from sqlalchemy.testing.provision import update_db_opts


@temp_table_keyword_args.for_db("cockroachdb")
def _cockroachdb_temp_table_keyword_args(cfg, eng):
    return {"prefixes": ["TEMPORARY"]}


@update_db_opts.for_db("cockroachdb")
def _update_db_opts(db_url, db_opts, options):
    """Set database options (db_opts) for a test database that we created."""
    db_opts["isolation_level"] = "SERIALIZABLE"
