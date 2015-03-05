#!/usr/bin/env python2.7

from setuptools import setup, find_packages

setup(
    name="Modelstatus",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "unittest2==0.8.0",
        "Cython==0.21.2",
        "SQLAlchemy==0.9.8",
        "argparse==1.2.1",
        "extras==0.0.3",
        "falcon==0.1.10",
        "psycopg2==2.5.4",
        "python-dateutil==2.4.0",
        "python-mimeparse==0.1.4",
        "pyzmq==14.5.0",
        "six==1.9.0",
        "testtools==1.5.0",
        "wsgiref==0.1.2",
    ],
)
