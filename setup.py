from setuptools import setup

install_requires = [
    'psycopg2',
]

setup(
    name='cockroach',
    packages=['cockroach', 'cockroach.sqlalchemy'],
    install_requires=install_requires,
    entry_points={
        'sqlalchemy.dialects': [
            'cockroach = cockroach.sqlalchemy.dialect:CockroachDialect',
        ],
    },
)
