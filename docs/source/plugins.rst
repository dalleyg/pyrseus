.. _plugins:

#######################
Plugins for ExecutorCtx
#######################

The following is a comprehensive list of all of the
`~pyrseus.ctx.mgr.ExecutorCtx` and `~pyrseus.interactive.get_executor` plugins
that are provided by Pyrseus.

Serial-only Plugins
===================

These serial-only plugins are non-concurrent ones that are suitable for light
workloads and troubleshooting. They allow users to retain their executor-based
control flow instead of requiring users to have separate serial and parallel
driver functions. See the notebooks linked on the left for usage examples.

.. list-table::
    :header-rows: 1

    * - Plugin
      - Executor
      - Pickler
      - Errors at
      - Notes


    * - `~pyrseus.ctx.plugins.inline`
      - `~pyrseus.executors.inline.InlineExecutor`
      - n/a
      - ``fut.result()``
      - serial mode, useful for light workloads and debugging

    * - `~pyrseus.ctx.plugins.pinline`
      - `~pyrseus.executors.pinline.PInlineExecutor`
      - `pickle`
      - ``fut.result()``
      - like ``"inline"``, but simulates the pickling done by concurrent
        executors that use `pickle`; for troubleshooting pickling problems

    * - `~pyrseus.ctx.plugins.cpinline`
      - `~pyrseus.executors.cpinline.CpInlineExecutor`
      - |cloudpickle|_
      - ``fut.result()``
      - like ``"pinline"``, but uses |cloudpickle|_


    * - `~pyrseus.ctx.plugins.nocatch`
      - `~pyrseus.executors.nocatch.NoCatchExecutor`
      - n/a
      - ``exe.submit(...)``
      - like ``"inline"``, but has intentional non-standard exception handling

    * - `~pyrseus.ctx.plugins.pnocatch`
      - `~pyrseus.executors.pnocatch.PNoCatchExecutor`
      - `pickle`
      - ``exe.submit(...)``
      - ``"nocatch"``-style exception handling with ``"pinline"``-style pickle
        testing

    * - `~pyrseus.ctx.plugins.cpnocatch`
      - `~pyrseus.executors.cpnocatch.CpNoCatchExecutor`
      - |cloudpickle|_
      - ``exe.submit(...)``
      - ``"nocatch"``-style exception handling with ``"cpinline"``-style pickle
        testing


Single-host Concurrent Plugins
==============================

When only single-host parallelism is needed, the following plugins provide some
benefits over the built-in `~concurrent.futures.ThreadPoolExecutor` and
`~concurrent.futures.ProcessPoolExecutor`.

.. list-table::
    :header-rows: 1

    * - Plugin
      - Executor
      - Pickler
      - Errors at
      - Notes


    * - `~pyrseus.ctx.plugins.thread`
      - `~concurrent.futures.ThreadPoolExecutor`
      - n/a
      - ``fut.result()``
      - built-in executor, but this plugin's default thread count respects the
        cpu affinity mask


    * - `~pyrseus.ctx.plugins.process`
      - `~concurrent.futures.ProcessPoolExecutor`
      - n/a
      - ``fut.result()``
      - built-in executor, but this plugin's default process count respects the
        cpu affinity mask

    * - `~pyrseus.ctx.plugins.cpprocess`
      - `~pyrseus.executors.cpprocess.CpProcessPoolExecutor`
      - |cloudpickle|_
      - ``fut.result()``
      - ``"process"``, but uses |cloudpickle|_ for serialization


    * - `~pyrseus.ctx.plugins.loky`
      - misc from |loky|_
      - |cloudpickle|_
      - ``fut.result()``
      - misc interface tweaks to |loky|_'s ``ProcessPoolExecutor``


Multi-host-capable Concurrent Plugins
=====================================

The following plugins can support more advanced features like multi-host
parallelism. Note that actually enabling multi-host support can require
significant effort to configure the underlying 3rd party libraries. Advice on
such configuration is outside the scope of the Pyrseus project.

.. list-table::
    :header-rows: 1

    * - Plugin
      - Executor
      - Pickler
      - Errors at
      - Notes


    * - `~pyrseus.ctx.plugins.ipyparallel`
      - misc from |ipyparallel|_
      - |cloudpickle|_
      - ``fut.result()``
      - creates an executor from ``Cluster`` constructor parameters, a
        ``Cluster`` object, or a ``Client`` object


    * - `~pyrseus.ctx.plugins.mpi4py`
      - `~mpi4py.futures.MPIPoolExecutor`
      - `pickle`
      - ``fut.result()``
      - uses the powerful but heavyweight MPI framework via the |mpi4py|_
        package

    * - `~pyrseus.ctx.plugins.cpmpi4py`
      - an `~mpi4py.futures.MPIPoolExecutor` wrapper
      - |cloudpickle|_
      - ``fut.result()``
      - ``"mpi4py"``, but uses |cloudpickle|_ for serialization

Changing the Default Plugins
============================

To change the default serial and/or concurrent plugin, use
`~pyrseus.ctx.registry.SetDefaultExecutorPluginCtx` or
`~pyrseus.ctx.registry.set_default_executor_plugin`. When using the latter,
there are several natural places to do so:

- in a try-finally block that reverts the change when done (hint: consider using
  `~pyrseus.ctx.registry.SetDefaultExecutorPluginCtx` instead for these use
  cases),

- at the start of each applicable script and/or notebook,

- in an `IPython startup file
  <https://ipython.readthedocs.io/en/stable/interactive/tutorial.html#startup-files>`_,

- in your `usercustomize
  <https://docs.python.org/3/library/site.html#module-usercustomize>`_ module,
  or

- in your `sitecustomize
  <https://docs.python.org/3/library/site.html#module-sitecustomize>`_ module.
