#!/usr/bin/env python2.7

from setuptools import setup, find_packages

setup(
    name="Syncer",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests==2.5.0",
        "python-dateutil==1.5.0"
    ],
)
