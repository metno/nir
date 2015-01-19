#!/bin/bash

# Installs virtualenv if needed, install packages and run all tests
# found in modelstatus/tests

set -e

if [ ! -d virtualenv ]
then
    virtualenv virtualenv
fi

source virtualenv/bin/activate

pip install -r requirements.txt

bn=`basename $0`
dirname=`dirname $bn`
export PYTHONPATH=`readlink -m $dirname/..`

cd tests

python -m unittest discover
