#########################################################
Pyrseus: Serial Executors and the ``ExecutorCtx`` Factory
#########################################################

Pyrseus extends Python's `concurrent.futures` asynchronous and concurrent
programming package with:

- a collection of non-concurrent executors for light workloads and
  troubleshooting,
- `~pyrseus.ctx.mgr.ExecutorCtx`, a factory for easily switching between
  different executors, and
- a collection of ready-built `~pyrseus.ctx.mgr.ExecutorCtx` plugins, supporting
  executors from `concurrent.futures`, |ipyparallel|_, |loky|_, |mpi4py|_, and
  itself. Where relevant, optional |cloudpickle|_-enhanced plugins are also
  provided.

.. toctree::
   :hidden:

   Pyrseus Home<self>
   install
   plugins
   contributing

General Information
===================

- :doc:`install`

- :doc:`plugins`

- :doc:`contributing`

API Documentation
=================

.. autosummary::
   :toctree: _autosummary
   :template: custom-module-template.rst
   :recursive:

   pyrseus

Usage Examples
==============

.. toctree::
   :maxdepth: 2
   :caption: Notebooks

   notebooks/executor_ctx_vs_classes
   notebooks/debugging_example
   notebooks/on_error
   notebooks/get_executor

TODO
====

 - [ ] get rid of the meta executor and plugin system
  - [ ] update
      - [ ] toplevel
        - [x] optional-non-win32-requirements.txt (deleted)
        - [x] optional-requirements.txt (deleted)
        - [x] pyproject.toml (simplified)
        - [ ] README.md: simplify
        - [x] requirements.txt (simplified)
      - [ ] docs/source/
        - [ ] conf.py
        - [x] contributing.rst
        - [ ] index.rst
        - [x] install.rst
        - [ ] plugins.rst
        - [x] writingplugins.rst (deleted)
        - [ ] notebooks/*
      - [x] src/pyrseus/
        - [x] interactive.py (deleted)
        - [x] __init__.py
        - [x] core/
          - [x] pickle.py
          - [x] sys.py
        - [x] executors/*
        - [x] ctx/ (deleted all, including OnError stuff for now)
        - [x] tests/*
  - [ ] grep
      - [ ] plugin
      - [ ] ExecutorCtx
      - [ ] loky
      - [ ] mpi4py
      - [ ] ipyparallel
      - [ ] OnError
  - [ ] release notes
      - [x] bump version number
