from setuptools import setup

setup(
    name='sqlalchemy-cockroachdb',
    version='0.4.0',
    author='Cockroach Labs',
    author_email='cockroach-db@googlegroups.com',
    url='https://github.com/cockroachdb/sqlalchemy-cockroachdb',
    description='CockroachDB adapter for SQLAlchemy',
    license="http://www.apache.org/licenses/LICENSE-2.0",
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        ],

    packages=['cockroachdb', 'cockroachdb.sqlalchemy'],
    entry_points={
        'sqlalchemy.dialects': [
            'cockroachdb = cockroachdb.sqlalchemy.dialect:CockroachDBDialect',
        ],
    },
)
