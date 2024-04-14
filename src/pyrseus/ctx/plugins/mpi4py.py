"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a simple factory for the 3rd
party `~mpi4py.futures.MPIPoolExecutor`, without overriding any of its defaults.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "mpi4py" works with normally-picklable tasks and return values
>>> skip_if_unavailable("mpi4py")
>>> with ExecutorCtx("mpi4py", 1) as exe:
...     assert exe.submit(os.getpid).result() != os.getpid()

>>> # But "mpi4py" doesn't use cloudpickle like "cpmpi4py" does,
>>> # so it can't handle fancy things like lambdas.
>>> skip_if_unavailable("mpi4py")
>>> with ExecutorCtx("mpi4py") as exe:
...     exe.submit(lambda: os.getpid()).result()
Traceback (most recent call last):
...
_pickle.PicklingError: Can't pickle ...

Note that unlike `multiprocessing`-based executors, the |mpi4py|_-based ones
only replicate the ``PYTHONPATH`` environment variable by default in workers,
not `sys.path`. If you have made local modifications to your `sys.path` list, be
sure to replicate them via the ``path`` argument that's passed to
`~.mpi4py.futures.MPIPoolExecutor`.

Plugin-specific Notes
---------------------

- *Common Use Cases:* Where one would otherwise use
  `~pyrseus.ctx.plugins.cpmpi4py` but where one wants to avoid depending on
  |cloudpickle|_.

- *Concurrency:* determined by the user's MPI configuration.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* |mpi4py|_

- *Underlying Executor:* `.mpi4py.futures.MPIPoolExecutor`

- *Default max_workers:* determined by the user's MPI configuration. Often this
  will require giving an explicit limit instead of relying on a pre-configured
  default.

- *Pickling:* `pickle`

- *OnError handling:* Fully supports `~pyrseus.ctx.api.OnError.WAIT` and
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` modes.
  `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode is automatically downgraded to
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES`.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""

from functools import cached_property
from typing import Optional

from pyrseus.core.sys import module_exists
from pyrseus.ctx.api import ExecutorPluginEntryPoint, OnError, OnErrorLike
from pyrseus.ctx.simple import OnExitProxy


class EntryPoint(ExecutorPluginEntryPoint):
    supports_serial = False
    supports_concurrent = True

    @cached_property
    def is_available(self) -> bool:
        return module_exists("mpi4py.futures")

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
        on_error: OnErrorLike = OnError.CANCEL_FUTURES,
        **kwargs,
    ):
        from mpi4py.futures import MPIPoolExecutor

        on_error = OnError.get(on_error)
        if on_error is OnError.KILL_WORKERS:
            # For now, demote KILL_WORKERS to CANCEL_FUTURES. MPIPoolExecutor
            # doesn't directly support a kill_workers mode. With the help of an
            # MPI expert, we'll be happy to consider adding a kill feature once
            # we know the right way to do it with MPI.
            on_error = OnError.CANCEL_FUTURES

        # Future work: if there's demand, add support for MPICommExecutor,
        # mpi4py's ThreadPoolExecutor, and/or mpi4py's ProcessPoolExecutor.
        ctx = MPIPoolExecutor(max_workers, **kwargs)

        if on_error is OnError.WAIT:
            return ctx, None
        elif on_error is OnError.CANCEL_FUTURES:
            exe = ctx.__enter__()  # just to be pedantic
            return OnExitProxy(exe, on_error=on_error, shutdown=True), None
        else:
            raise NotImplementedError(on_error)


ENTRY_POINT = EntryPoint()
