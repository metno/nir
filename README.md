NIR
===

## Abstract

NIR is a model metadata web service and WDB data synchronization system.

Please see the README for [Syncer](syncer/README.md) and [Modelstatus](modelstatus/README.md).

## Development Environment

### Arcanist Lint

flake8 is required to run `arc lint`. Run ```sudo pip install flake8``` in Ubuntu Debian.

### System packages

To run Syncer, you'll need to install the package `python-lxml` through your package manager.

### Setup the environment

To setup and install requirements, run `syncer/setup.py develop` and `modelstatus/setup.py develop`.
