apilevel = '2.0'

# TODO(bdarnell): how thread-safe are we?
threadsafety = 1

# The DB-API has a complex scheme for allowing drivers to choose
# which type of placeholders may be used. Unfortunately none of them
# match our use of `$1`, and the only one we can implement without
# client-side parsing of SQL is 'pyformat'.
paramstyle = 'pyformat'

__all__ = ['apilevel', 'threadsafety', 'paramstyle']
