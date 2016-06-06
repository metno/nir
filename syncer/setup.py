#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="Syncer",
    version="0.1",
    scripts=['bin/syncer', 'bin/syncerctl'],
    packages=find_packages(),
    install_requires=[
        "lxml==3.6.0",
        "productstatus-client==6.0.3",
        "statsd==3.2.1",
        "psycopg2==2.6.1"
    ],
)
