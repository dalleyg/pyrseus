Pyrseus: Serial Executors and the `ExecutorCtx` Factory
======================================================

Pyrseus extends Python's `concurrent.futures` asynchronous and concurrent
programming package with

 - a collection of non-concurrent executors for light workloads and
   troubleshooting,
 - `ExecutorCtx`, a factory for easily switching between different executors,
   and
 - a collection of ready-built `ExecutorCtx` plugins, supporting executors from
   [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html),
   [ipyparallel](https://ipyparallel.readthedocs.io),
   [loky](https://loky.readthedocs.io), [mpi4py](https://mpi4py.readthedocs.io),
   and itself. Where relevant, optional
   [cloudpickle](https://github.com/cloudpipe/cloudpickle)-enhanced plugins are
   also provided.

Installation
------------

Pyrseus supports Linux, macOS, and Windows.

To install just Pyrseus and no plugins for 3rd party executors, run:

    python -m pip install pyrseus

To ensure that the [ipyparallel](https://ipyparallel.readthedocs.io),
[loky](https://loky.readthedocs.io), and/or
[mpi4py](https://mpi4py.readthedocs.io) plugins are also ready to use, run a
command like the following, removing the names of any plugins you don't need::

    python -m pip install 'pyrseus[ipyparallel,loky,mpi4py]'

Note that Pyrseus will auto-detect those packages, so if they're installed
through other means, then the relevant Pyrseus plugins will be automatically
enabled.

For additional instructions, see the [installation
guide](https://pyrseus.readthedocs.io/en/latest/install.html).

Full Documentation
------------------

For full documenation, see the [Pyrseus
Documentation](https://pyrseus.readthedocs.io/). It includes installation
instructions, a detailed summary of all of the executor plugins, guidance for
writing your own plugins, API documenation, and several notebooks showing
example use cases.
