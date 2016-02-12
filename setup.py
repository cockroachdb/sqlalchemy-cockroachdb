from setuptools import setup

install_requires = [
    'psycopg2',
]

setup(
    name='cockroach',
    packages=['cockroach'],
    install_requires=install_requires,
)
