from cockroach.http_sender import HTTPSender

from .cursor import Cursor
from . import errors
from . import types
from .wire_pb2 import Request


class Connection(object):
    def __init__(self, addr, user, database=None, auto_create=False):
        self._sender = HTTPSender(addr)
        self._user = user
        self._database = database
        self._closed = False
        self._session = None

        if self._database is not None:
            if auto_create:
                # TODO(bdarnell): create database doesn't take param?
                self._send_request("CREATE DATABASE IF NOT EXISTS %s" %
                                   self._database)
            self._send_request("SET DATABASE = %(db)s", dict(db=self._database))

    def close(self):
        self._check_closed()
        self._sender.close()
        self._closed = True

    def commit(self):
        self._check_closed()

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self)

    def _check_closed(self):
        if self._closed:
            raise errors.Error("connection is closed")

    def _send_request(self, stmt, params=None):
        self._check_closed()

        req = Request(user=self._user)
        if self._session is not None:
            req.session = self._session
        if params is not None:
            # Transform the 'pyformat' query to our own '$1' placeholders.
            param_map = {}
            for key, in_param in sorted(params.items()):
                req.params.extend([types._python_to_datum(in_param)])
                param_map[key] = '$%d' % len(req.params)
            stmt = stmt % param_map
        req.sql = stmt

        resp = self._sender.send(req)
        self._session = resp.session
        return resp


# According to an "optional extension" of PEP 249 (which is enforced by the
# compliance test), all error classes must also be available on the class
# namespace.
for e in errors.__all__:
    setattr(Connection, e, getattr(errors, e))


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

__all__ = ['Connection', 'connect']
