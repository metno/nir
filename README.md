NIR - Deprecated and not maintained anymore
===========================================

NIR is not in use by MET Norway and we have stopped supporting this software.
The repository is archived for reference.
Please notice that dependencies and code is outdated, and that you must expect that the current state is that this repository has bugs and depends on library versions with known security weaknesses.


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
