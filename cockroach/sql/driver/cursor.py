import sys

from . import errors
from . import types
from .wire_pb2 import Response


class Cursor(object):
    def __init__(self, conn):
        self.conn = conn
        self.arraysize = None
        self._result = None
        self._pos = None

    @property
    def rowcount(self):
        if self._result is None:
            return None
        which = self._result.WhichOneof("union")
        if which == "rows_affected":
            return self._result.rows_affected
        elif which == "rows":
            return len(self._result.rows.rows)
        elif which == "ddl":
            return -1
        else:
            raise errors.InterfaceError("unsupported result type %s", which)

    @property
    def description(self):
        if self._result is None:
            return None
        which = self._result.WhichOneof("union")
        if which != "rows":
            return None
        cols = self._result.rows.columns
        # Description tuples are (name, type_code, display_size,
        # internal_size, precision, scale, null_ok); all but the first
        # two are optional.
        # TODO(bdarnell): return correct type when the server returns types.
        return [(c, types.STRING, None, None, None, None, None) for c in cols]

    def _send_request(self, stmt, params):
        resp = self.conn._send_request(stmt, params)
        if len(resp.results) != 1:
            # TODO(bdarnell): we could support multi-result queries with
            # the nextset() method.
            raise errors.NotSupportedError("expected 1 result, got %s",
                                           len(resp.results))
        self._result = resp.results[0]
        self._pos = 0

    def execute(self, stmt, params=None):
        self._send_request(stmt, params)

    def executemany(self, stmt, seq_of_params):
        total = 0
        for params in seq_of_params:
            self.execute(stmt, params)
            rowcount = self.rowcount
            if rowcount > 0:
                total += rowcount
        self._result = Response.Result(rows_affected=total)

    def fetchone(self):
        rows = self.fetchmany(1)
        if len(rows) == 1:
            return rows[0]
        elif len(rows) == 0:
            return None
        else:
            raise errors.DataError("expected 1 row, got %d" % len(rows))

    def fetchmany(self, size=None):
        if self._result is None or self._result.WhichOneof("union") != "rows":
            raise errors.ProgrammingError("no results available")
        if size is None:
            size = self.arraysize or 1
        # The double-slicing here is to avoid overflow when
        # self._pos + size > sys.maxsize.
        in_rows = self._result.rows.rows[self._pos:][:size]
        self._pos += len(in_rows)
        out_rows = []
        for in_row in in_rows:
            out_row = tuple(types._datum_to_python(d) for d in in_row.values)
            out_rows.append(out_row)
        return out_rows

    def fetchall(self):
        return self.fetchmany(sys.maxsize)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
