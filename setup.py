from setuptools import setup
import sys

install_requires = [
    # protobuf 2.6 doesn't support python 3.
    'protobuf==3.0.0-alpha-1',
    'requests',
]

if sys.version_info < (3, 4):
    install_requires.append('enum34')

setup(
    name='cockroach',
    packages=['cockroach', 'cockroach.sql.driver'],
    install_requires=install_requires,
    extras_require={'tests': ['werkzeug']},
)
