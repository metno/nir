#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="Syncer",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "lxml==3.6.0",
        "productstatus-client==5.3.0",
        "statsd==3.2.1"
    ],
)
