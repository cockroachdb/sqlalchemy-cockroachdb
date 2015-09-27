import datetime
import time

try:
    import enum
except ImportError:
    import enum34 as enum

class Binary(object):
    def __init__(self, v):
        self.v = v


def Date(y, m, d):
    return datetime.date(y, m, d)


def Time(h, m, s):
    return datetime.time(h, m, s)


def Timestamp(y, mo, d, h, mi, s):
    return datetime.datetime(y, mo, d, h, mi, s)


# Implementations of the FromTicks methods are copied from PEP 249.
def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


class TypeCode(enum.Enum):
    STRING = 1
    BINARY = 2
    NUMBER = 3
    DATETIME = 4
    ROWID = 5

for k, v in TypeCode.__members__.items():
    globals()[k] = v
