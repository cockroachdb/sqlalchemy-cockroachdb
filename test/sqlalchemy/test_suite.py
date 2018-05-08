from sqlalchemy.testing.suite import *  # noqa

# Depends on ESCAPE modifier to LIKE.
del LikeFunctionsTest

# These tests fail on sqlalchemy 1.2, but either passed or didn't
# exist in 1.1.
del ComponentReflectionTest.test_get_unique_constraints
del ComponentReflectionTest.test_get_noncol_index_pk
