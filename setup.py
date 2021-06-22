import os
import re

from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), "sqlalchemy_cockroachdb", "__init__.py")) as v:
    VERSION = re.compile(r'.*__version__ = "(.*?)"', re.S).match(v.read()).group(1)

with open(os.path.join(os.path.dirname(__file__), "README.md")) as f:
    README = f.read()

setup(
    name="sqlalchemy-cockroachdb",
    version=VERSION,
    author="Cockroach Labs",
    author_email="cockroach-db@googlegroups.com",
    url="https://github.com/cockroachdb/sqlalchemy-cockroachdb",
    description="CockroachDB dialect for SQLAlchemy",
    long_description=README,
    long_description_content_type="text/markdown",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="SQLAlchemy CockroachDB",
    project_urls={
        "Documentation": "https://github.com/cockroachdb/sqlalchemy-cockroachdb/wiki",
        "Source": "https://github.com/cockroachdb/sqlalchemy-cockroachdb",
        "Tracker": "https://github.com/cockroachdb/sqlalchemy-cockroachdb/issues",
    },
    packages=find_packages(include=["sqlalchemy_cockroachdb"]),
    include_package_data=True,
    install_requires=["SQLAlchemy"],
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "cockroachdb.psycopg2 = sqlalchemy_cockroachdb.psycopg2:CockroachDBDialect_psycopg2",
            "cockroachdb = sqlalchemy_cockroachdb.psycopg2:CockroachDBDialect_psycopg2",
        ],
    },
)
