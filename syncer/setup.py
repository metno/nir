#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="Syncer",
    version="0.1",
    scripts=['bin/syncer', 'bin/syncerctl'],
    packages=find_packages(),
    install_requires=[
        "lxml==3.6.1",
        "productstatus-client==6.0.7",
        "statsd==3.2.1"
    ],
)
