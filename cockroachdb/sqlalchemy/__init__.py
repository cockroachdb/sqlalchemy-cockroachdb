from sqlalchemy import util
from sqlalchemy_cockroachdb import run_transaction  # noqa

util.warn_limited(
    "Importing ``run_transaction`` from ``cockroachdb.sqlalchemy`` is deprecated since "
    "version %s of this dialect. "
    "Please import it from ``sqlalchemy_cockroachdb`` instead.",
    "1.4",
)
