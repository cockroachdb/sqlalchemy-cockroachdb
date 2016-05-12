from setuptools import setup

install_requires = [
    'psycopg2',
]

setup(
    name='cockroachdb',
    packages=['cockroachdb', 'cockroachdb.sqlalchemy'],
    install_requires=install_requires,
    entry_points={
        'sqlalchemy.dialects': [
            'cockroachdb = cockroachdb.sqlalchemy.dialect:CockroachDBDialect',
        ],
    },
)
