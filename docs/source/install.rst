.. _install:

##################
Installing Pyrseus
##################

Pyrseus supports Linux, macOS, and Windows.

Using pip
=========

To install Pyrseus, run::

    python -m pip install pyrseus

From a Git Clone
================

Alternatively, you can clone the Pyrseus repository and either use it directly
by adding its ``src/`` directory to your ``PYTHONPATH`` or by installing it from
a source clone. Here's how to do the latter::

    # Clone the repository.
    git clone git@github.com:dalleyg/pyrseus.git
    cd pyrseus

    # Install dependencies.
    python -m pip install -r requirements.txt
    python -m pip install -r dev-requirements.txt

    # Build the .whl and sdist tarballs.
    hatch build

    # Install to your Python interpreter.
    python -m pip --find-links=dist pyrseus
