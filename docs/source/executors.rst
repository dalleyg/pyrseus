.. _executors:

#########################
Pyrseus' Executor Classes
#########################

The following is a comprehensive list of all of the
`concurrent.futures.Executor` subclasses provided by Pyrseus.

Serial-only Executors
=====================

These serial-only executors non-concurrent ones that are suitable for light
workloads and troubleshooting. They allow users to retain their executor-based
control flow instead of requiring users to have separate serial and parallel
driver functions. See the notebooks linked on the left for usage examples.

.. list-table::
    :header-rows: 1

    * - Executor
      - Pickler
      - Errors at
      - Notes


    * - `~pyrseus.executors.inline.InlineExecutor`
      - n/a
      - ``fut.result()``
      - serial mode, useful for light workloads and debugging

    * - `~pyrseus.executors.pinline.PInlineExecutor`
      - `pickle`
      - ``fut.result()``
      - like ``"inline"``, but simulates the pickling done by concurrent
        executors that use `pickle`; for troubleshooting pickling problems

    * - `~pyrseus.executors.cpinline.CpInlineExecutor`
      - |cloudpickle|_
      - ``fut.result()``
      - like ``"pinline"``, but uses |cloudpickle|_


    * - `~pyrseus.executors.nocatch.NoCatchExecutor`
      - n/a
      - ``exe.submit(...)``
      - like ``"inline"``, but has intentional non-standard exception handling

    * - `~pyrseus.executors.pnocatch.PNoCatchExecutor`
      - `pickle`
      - ``exe.submit(...)``
      - ``"nocatch"``-style exception handling with ``"pinline"``-style pickle
        testing

    * - `~pyrseus.executors.cpnocatch.CpNoCatchExecutor`
      - |cloudpickle|_
      - ``exe.submit(...)``
      - ``"nocatch"``-style exception handling with ``"cpinline"``-style pickle
        testing


Single-host Concurrent Executors
================================

When only single-host parallelism is needed, the following executor provides
some benefits over the built-in `~concurrent.futures.ProcessPoolExecutor`.

.. list-table::
    :header-rows: 1

    * - Executor
      - Pickler
      - Errors at
      - Notes

    * - `~pyrseus.executors.cpprocess.CpProcessPoolExecutor`
      - |cloudpickle|_
      - ``fut.result()``
      - ``"process"``, but uses |cloudpickle|_ for serialization
