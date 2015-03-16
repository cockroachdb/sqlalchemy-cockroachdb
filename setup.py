from distutils.core import setup

setup(
    name='cockroach',
    packages=['cockroach', 'cockroach.proto'],
    install_requires=[
        # protobuf 2.6 doesn't support python 3.
        'protobuf==3.0.0-alpha-1',
    ],
)
