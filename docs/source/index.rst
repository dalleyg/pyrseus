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
   writingplugins
   contributing

General Information
===================

- :doc:`install`

- :doc:`plugins`

- :doc:`writingplugins`

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
      - [ ] optional-non-win32-requirements.txt: break up?
      - [ ] optional-requirements.txt: break up?
      - [ ] pyproject.toml: rework optionals
      - [ ] README.md: simplify
      - [ ] requirements.txt: make cloudpickle optional, document what uses this file
    - [ ] docs/source/
      - [ ] contributing.rst
      - [ ] index.rst
      - [ ] install.rst
      - [ ] plugins.rst
      - [ ] writingplugins.rst (delete)
      - [ ] notebooks/*
    - [ ] src/pyrseus/
      - [x] interactive.py: delete
      - [ ] __init__.py: decide how to handle optional executors that might not
            be installed
      - [ ] core/
        - [x] pickle.py
      - [ ] executors/*
      - [ ] ctx/
        - [ ] plugins/: migrate
        - [ ] *.py: delete
          - [ ] api.py: migrate OnError stuff if it's still relevant
      - [ ] tests/*
 - [ ] grep for plugin and ExecutorCtx
