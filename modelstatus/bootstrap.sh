#!/bin/bash
#
# This script deploys the Modelstatus application within the Python virtualenv
# specified on the command line.
#

set -e

# Path to virtual environment for the application
venvpath=$1

if [ ! -d "$venvpath" ]; then
    echo "First argument must be a Python virtualenv directory."
    echo "Usage: $0 <path-to-virtualenv>"
    exit 1
fi

# Load virtualenv
source "$venvpath/bin/activate"

# Install dependencies
`dirname $0`/setup.py install

# Unload virtualenv
deactivate
