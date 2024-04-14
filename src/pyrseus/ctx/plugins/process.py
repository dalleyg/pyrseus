"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a factory for the built-in
`~concurrent.futures.ProcessPoolExecutor` that respects the CPU affinity mask
when choosing the default worker process count.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "process" works with normally-picklable tasks and return values
>>> skip_if_unavailable("process")
>>> with ExecutorCtx("process", 1) as exe:
...     assert exe.submit(os.getpid).result() != os.getpid()

>>> # But "process" doesn't use cloudpickle like "cpprocess" does,
>>> # so it can't handle fancy things like lambdas.
>>> skip_if_unavailable("process")
>>> with ExecutorCtx("process") as exe:
...     exe.submit(lambda: os.getpid()).result()
Traceback (most recent call last):
...
_pickle.PicklingError: Can't pickle ...

Plugin-specific Notes
---------------------

- *Common Use Cases:* As replacement for directly using
  `concurrent.futures.ProcessPoolExecutor` for users that enjoy using
  `~pyrseus.ctx.mgr.ExecutorCtx` and/or want to have a more conservative default
  ``max_workers``.

- *Concurrency:* Each worker runs in its own process.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* This plugin has no 3rd party dependencies.

- *Underlying Executor:* `concurrent.futures.ProcessPoolExecutor`

- *Default max_workers*:* Uses `~pyrseus.core.sys.get_num_available_cores`
  instead of `multiprocessing.cpu_count`, respecting the CPU affinity mask when
  possible. Currently is unaware of cgroups constraints.

- *Pickling:* `pickle`.

- *OnError handling:* Fully supports `~pyrseus.ctx.api.OnError.WAIT` and
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` modes.
  `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode is automatically downgraded to
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES`.

  - By default, `~concurrent.futures.ProcessPoolExecutor`, pre-queues one extra
    task, making it uncancellable; so in
    `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` mode, that pre-queued task will
    still be run.

.. note::

   If you can afford to add two extra 3rd party dependencies, consider using the
   `~pyrseus.ctx.plugins.loky` plugin instead of this one. Here are a few of its
   advantages:

   - Like our `~pyrseus.ctx.plugins.cpprocess` plugin, it uses the more powerful
     |cloudpickle|_ for pickling instead of just `pickle`.

   - Its `~.loky.backend.context.cpu_count` function for determining the default
     ``max_workers`` is more sophisticated than ours, e.g. it respects not only
     the CPU affinity mask, but also cgroups constraints.

   - Even though the |loky|_ project has already led to significant improvements
     to the built-in `multiprocessing` library, its executors still handle a few
     worker crash situations better than the built-in
     `~concurrent.futures.ProcessPoolExecutor` that this plugin uses (as of
     Python 3.10).

   - It supports reusable worker pools that are especially convenient to use in
     interactive code.

   - It supports the `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode for faster
     teardowns when there are uncaught exceptions in the main program.

See :doc:`../plugins` for a summary of related plugins, and installation notes.
"""


import sys
from concurrent.futures import ProcessPoolExecutor
from functools import cached_property
from typing import Optional

from pyrseus.core.sys import get_num_available_cores
from pyrseus.ctx.api import (
    ExecutorPluginEntryPoint,
    OnError,
    OnErrorLike,
    extract_keywords,
)
from pyrseus.ctx.simple import OnExitProxy, shutdown_cancel_on_error


class EntryPoint(ExecutorPluginEntryPoint):
    supports_serial = False
    supports_concurrent = True
    is_available = True

    ExecutorCls = ProcessPoolExecutor

    @cached_property
    def allowed_keywords(self):
        return extract_keywords(self.create) | extract_keywords(self.ExecutorCls)

    def create(
        self,
        max_workers: Optional[int] = None,
        *,
        on_error: OnErrorLike = OnError.KILL_WORKERS,
        **kwargs,
    ):
        # Preprocess args.
        if max_workers is None:
            max_workers = max(1, get_num_available_cores())
        on_error = OnError.get(on_error)

        # ProcessPoolExecutor doesn't have a public API for cleanly killing
        # workers, so we downgrade on_error to cancel mode.
        if on_error is OnError.KILL_WORKERS:
            on_error = OnError.CANCEL_FUTURES

        # Create, based on the desired on_error mode.
        if on_error is OnError.WAIT:
            ctx = self.ExecutorCls(max_workers, **kwargs)
            pre_exit = None
        elif on_error is OnError.CANCEL_FUTURES:
            # Unfortunately, under some conditions shutdown(cancel_futures=True)
            # causes deadlocks on CPython <3.12
            # (https://github.com/python/cpython/issues/116685). So we fallback
            # to emulating is behavior in our code.
            if sys.version_info < (3, 12, 2):
                orig_ctx = self.ExecutorCls(max_workers, **kwargs)
                exe = orig_ctx.__enter__()
                ctx = OnExitProxy(exe, on_error=on_error, shutdown=False)
                pre_exit = None
            else:
                ctx = self.ExecutorCls(max_workers, **kwargs)
                pre_exit = shutdown_cancel_on_error
        else:
            raise ValueError(on_error)

        return ctx, pre_exit


ENTRY_POINT = EntryPoint()
