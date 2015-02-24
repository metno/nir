#!/bin/bash

set -e
# Script for bootstrapping the modelstatus application, .e.g make sure
# all the python package dependencies are installed in the virtualenv:
#
# 1. activate the virtualenv given as argument to the script
# 2. install packages listed in modelstatus/requirements.txt.

# Path to virtual environment for the application
venvpath=$1

if [ -z "$venvpath" ]
then
    echo "Missing argument to virtualenv directory, usage:"
    echo "./bootstrap.sh <path-to-virtualenv>"
    exit 1;
fi

if [ ! -d "$venvpath" ]
then
    virtualenv "$venvpath"
fi

source "$venvpath/bin/activate"

path_to_reqsfile=`dirname $0`
pip install -r "$path_to_reqsfile/requirements.txt"

deactivate
