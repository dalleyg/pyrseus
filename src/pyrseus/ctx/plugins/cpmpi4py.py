"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a wrapper around the 3rd
party `~mpi4py.futures.MPIPoolExecutor`, enhancing it to use |cloudpickle|_ for
pickling tasks and their return values, instead of `pickle`.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "cpmpi4py" can pickle fancy things like lambdas with cloudpickle
>>> skip_if_unavailable("cpmpi4py")
>>> with ExecutorCtx("cpmpi4py", 1) as exe:
...     assert exe.submit(lambda: os.getpid()).result() != os.getpid()

Note that unlike `multiprocessing`-based executors, the |mpi4py|_-based ones
only replicate the ``PYTHONPATH`` environment variable by default in workers,
not `sys.path`. If you have made local modifications to your `sys.path` list, be
sure to replicate them via the ``path`` argument that's passed to
`~.mpi4py.futures.MPIPoolExecutor`.

Plugin-specific Notes
---------------------

- *Common Use Cases:* For multi-process and multi-host workloads, especially
  for for organizations that already have MPI setup but want a simple map-only
  interface to it.

- *Concurrency:* determined by the user's MPI configuration.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* |cloudpickle|_, |mpi4py|_

- *Underlying Executor:* a thin wrapper around
  `.mpi4py.futures.MPIPoolExecutor` that makes it use |cloudpickle|_ instead of
  `pickle` for pickling tasks and their results.

- *Default max_workers:* determined by the user's MPI configuration. Often this
  will require giving an explicit limit instead of relying on a pre-configured
  default.

- *Pickling:* |cloudpickle|_

- *OnError handling:* Fully supports `~pyrseus.ctx.api.OnError.WAIT` and
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` modes.
  `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode is automatically downgraded to
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES`.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

import multiprocessing
from functools import cache, cached_property
from typing import Optional

from pyrseus.core.pickle import CustomPickledClosure
from pyrseus.core.sys import module_exists
from pyrseus.ctx.api import ExecutorPluginEntryPoint, OnError, OnErrorLike
from pyrseus.ctx.simple import OnExitProxy


@cache
def get_class():
    """
    Lazily creates the underlying executor class. This is done in a function
    because plugins must not import 3rd party dependencies at module load time.

    Note: we don't have to worry about the picklability of the executor class:
    executors are typically not picklable anyway.
    """
    # Deferring these imports is the only reason we lazily create the class in
    # this function.
    import cloudpickle
    from mpi4py.futures import MPIPoolExecutor

    class CpMPIPoolExecutor(MPIPoolExecutor):
        def submit(self, fcn, /, *args, **kwargs):
            closure = CustomPickledClosure(
                fcn,
                args,
                kwargs,
                dumps=cloudpickle.dumps,
                loads=cloudpickle.loads,
            )
            return super().submit(closure)

    return CpMPIPoolExecutor


def init_mp_authkey(authkey):
    multiprocessing.current_process().authkey = authkey


def get_mp_authkey():
    return multiprocessing.current_process().authkey


class EntryPoint(ExecutorPluginEntryPoint):
    supports_serial = False
    supports_concurrent = True

    @cached_property
    def is_available(self) -> bool:
        return module_exists("mpi4py.futures") and module_exists("cloudpickle")

    allowed_keywords = {
        # Those supported by mpi4py. Unfortunately, their executor constructor
        # takes **kwargs, so we can't use metaprogramming like we do for other
        # plugins like process and loky.
        "python_exe",
        "python_args",
        "mpi_info",
        "globals",
        "main",
        "path",
        "wdir",
        "env",
    }

    def create(
        self,
        max_workers: Optional[int] = None,
        on_error: OnErrorLike = OnError.KILL_WORKERS,
        **kwargs,
    ):
        on_error = OnError.get(on_error)
        if on_error is OnError.KILL_WORKERS:
            # For now, demote KILL_WORKERS to CANCEL_FUTURES. MPIPoolExecutor
            # doesn't directly support a kill_workers mode. With the help of an
            # MPI expert, we'll be happy to consider adding a kill feature once
            # we know the right way to do it with MPI.
            on_error = OnError.CANCEL_FUTURES

        cls = get_class()
        ctx = cls(max_workers, **kwargs)

        if on_error is OnError.WAIT:
            return ctx, None
        elif on_error is OnError.CANCEL_FUTURES:
            exe = ctx.__enter__()  # just to be pedantic
            return OnExitProxy(exe, on_error=on_error, shutdown=True), None
        else:
            raise NotImplementedError(on_error)


ENTRY_POINT = EntryPoint()
