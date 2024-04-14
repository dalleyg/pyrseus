.. _contributing:

#######################
Contributing to Pyrseus
#######################

We welcome contributions to the Pyrseus project. To do so, send us a `pull
request <https://help.github.com/articles/using-pull-requests/>`_.

Checklist
=========

When preparing a Pull Request for the Pyrseus repository, please be mindful of
the following guidance.

- Manage 3rd party dependencies.

  - If any extra 3rd party dependencies are needed, add them to

    - ``optional-dependencies.txt``, if your feature will work on all platforms,
      or
    - ``optional-non-win32-dependencies.txt``, if your feature only works on
      Linux (including under WSL) and macOS.

  - If any extra 3rd party dependencies are needed, also add them to
    ``docs/requirements.txt`` (always).

  - Limit any new 3rd party dependencies to being used in `pyrseus.ctx.plugins`.
    We'd like to keep the required set of dependencies small.

  - Use lazy imports for any 3rd party dependencies that aren't listed in
    ``requirements.txt``. E.g. it's fine to import |cloudpickle|_ at the top of
    any module, but imports of |ipyparallel|_, |loky|_, and |mpi4py|_ must
    always be done inside a function. Additionally, build-in plugins must not
    require importing any optional 3rd party dependencies to evaluate their
    ``ENTRY_POINT.is_available`` property.

- Fully test your changes (see below).

- Prepare your pull request (PR).

  - Clean up your code.

    - Use ``ruff check --fix .``.
    - Run `black <https://black.readthedocs.io>`_ on any ``.py`` files you
      added or created.

  - Document your change in the PR description.

    - Include a brief discussion of how you tested it and whether all of the
      tests succeeded.

  - Submit your PR.

Useful Commands
===============

The following are some types of commands you may wish to use when modifying the
Pyrseus repository:

- ``tox``: runs all of the unit tests. By default, this will run two rounds of
  testing: with all optional dependencies, and then again with just the base
  dependencies.

- ``tox -e py3.10-all-linux``: example of how to run just the Python 3.10 tests
  with ``all`` optional dependencies (vs. just the ``base`` dependencies) on
  Linux.

- ``tox -- -k test_ipyparallel_plugin``: an example of selecting one test file
  to run.

- ``tox -e html``: generates the HTML documentation for Pyrseus using Sphinx.
  Note that sometimes Sphinx's caches can get stale. If you're suspicious of
  that, run ``clean.sh`` first.

- ``tox -e ipython``: runs an IPython shell from a ``tox``-managed virtual
  environment that includes all of the optional dependencies.

- ``clean.sh``: deletes files and caches created by various ``tox`` commands

-  ``(cd docs/source/notebooks && jupyter lab 2>/dev/null) &``: manually runs
   Jupyter Lab for running and editing our documentation notebooks. Be sure to
   have `jupytext` installed first. Then when using one of the ``py`` files
   found in that starting directory, tell Jupyter to link it to an ``ipynb``
   file. The ``py`` files are checked into the repository. The ``ipynb`` files
   are more useful for interactive situations. With `jupytext` installed,
   Jupyter automatically keeps the two copies in sync.

- ``hatch build``: build the wheel and sdist tarball for Pyrseus.

Testing
=======

To perform final testing of your change, please do at least one of the
following:

- In your fork on github.com, run Pyrseus' "Test Python Packages" workflow. This
  is the easiest approach, but each test round uses a lot of GitHub minutes.

- Test your changes locally with both (a) Linux or macOS, and (b) Windows, as
  described below. This is the fastest, but it requires some extra setup.

Locally Simulating GitHub Workflows
-----------------------------------

To simulate the "Test Python Packages" workflow on a UNIX-like host, do the
following:

- First, setup `Docker <https://www.docker.com/get-started/>`_.

  - Note: Docker itself is unfortunately required by the ``nektos/act``
    framework used below. Lighter-weight alternatives like `Podman
    <https://podman.io/>`_ are not yet supported.

- Setup the `nektos/act <https://github.com/nektos/act>`_ system. The easiest
  way is to follow their `Act Installation Instructions
  <https://nektosact.com/installation/index.html>`_.

- To locally simulate the GitHub workflow, run a series of commands like the
  following, adjusting the ``--matrix`` options as appropriate for your base
  system.

  .. code-block:: text

     # Tests a minimal installation of Pyrseus
     $ act push -j build \
          --matrix os:ubuntu-latest \
          --matrix python-version:3.10 \
          --matrix include-optionals:false

     # Tests a full installation of Pyrseus, using all supported features on
     # the chosen platform.
     $ act push -j build \
          --matrix os:ubuntu-latest \
          --matrix python-version:3.10 \
          --matrix include-optionals:true

  Note that the output can be rather verbose, so you may wish to redirect it to
  a file for offline inspection.

- If your changes are supported on Windows, please also test them on Windows.
  Unfortunately, this procedure is less refined. See ``docker-win32/README.md``
  for details.
