#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="Syncer",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests==2.5.0",
        "python-dateutil==2.4.0",
        "pyzmq==14.5.0",
        "modelstatus-client==1.1.0",
    ],
)
