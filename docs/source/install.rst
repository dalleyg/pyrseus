.. _install:

##################
Installing Pyrseus
##################

Pyrseus supports Linux, macOS, and Windows. It is highly modular, allowing you
to install only the parts you actually need.

Using pip
=========

To install just Pyrseus and no plugins for 3rd party executors, run::

    python -m pip install pyrseus

To ensure that the |ipyparallel|_, |loky|_, and/or |mpi4py|_ plugins are also
ready to use, run a command like the following, removing the names of any
plugins you don't need::

    python -m pip install 'pyrseus[ipyparallel,loky,mpi4py]'

Note that Pyrseus will auto-detect those packages, so if they're installed
through other means, then the relevant Pyrseus plugins will be automatically
enabled.

From a Git Clone
================

Alternatively, you can clone the Pyrseus repository and either use it directly
by adding its ``src/`` directory to your ``PYTHONPATH`` or by installing it from
the source clone. Here's how to do the latter::

    # Clone the repository.
    git clone git@github.com:dalleyg/pyrseus.git
    cd pyrseus

    # Install dependencies.
    python -m pip install -r requirements.txt
    python -m pip install -r dev-requirements.txt
    python -m pip install -r optional-requirements.txt            # if desired
    python -m pip install -r optional-non-win32-requirements.txt  # if desired

    # Build the .whl and sdist tarballs.
    hatch build

    # Install to your Python interpreter.
    python -m pip --find-links=dist pyrseus
