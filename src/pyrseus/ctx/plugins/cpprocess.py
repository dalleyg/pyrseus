"""
`~pyrseus.ctx.mgr.ExecutorCtx` plugin that provides a wrapper around the
built-in `~concurrent.futures.ProcessPoolExecutor` that (a) uses |cloudpickle|_
instead of `pickle` for pickling tasks and their return values, and (b) respects
the CPU affinity mask when choosing the default thread count.

>>> import os
>>> from pyrseus.ctx.registry import skip_if_unavailable
>>> from pyrseus.ctx.mgr import ExecutorCtx

>>> # "cpprocess" can pickle fancy things like lambdas with cloudpickle
>>> skip_if_unavailable("cpprocess")
>>> with ExecutorCtx("cpprocess", 1) as exe:
...     assert exe.submit(lambda: os.getpid()).result() != os.getpid()

Plugin-specific Notes
---------------------

- *Common Use Cases:* For the same use cases as `~pyrseus.ctx.plugins.process`,
  but when users wish to use |cloudpickle|_ instead of `pickle` for serializing
  tasks and their results.

- *Concurrency:* Each worker runs in its own process.

- *Exceptions:* This plugin has standard exception-handling semantics: all
  task-related exceptions are captured in the task's future.

- *3rd Party Dependencies:* |cloudpickle|_

- *Underlying Executor:* `concurrent.futures.ProcessPoolExecutor`

- *Default max_workers*:* Uses `~pyrseus.core.sys.get_num_available_cores`
  instead of `multiprocessing.cpu_count`, respecting the CPU affinity mask when
  possible. Currently is unaware of cgroups constraints.

- *Pickling:* |cloudpickle|_

- *OnError handling:* Fully supports `~pyrseus.ctx.api.OnError.WAIT` and
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` modes.
  `~pyrseus.ctx.api.OnError.KILL_WORKERS` mode is automatically downgraded to
  `~pyrseus.ctx.api.OnError.CANCEL_FUTURES`.

  - By default, `~concurrent.futures.ProcessPoolExecutor`, pre-queues one extra
    task, making it uncancellable; so in
    `~pyrseus.ctx.api.OnError.CANCEL_FUTURES` mode, that pre-queued task will
    still be run.

.. note::

   If you can afford to add one extra 3rd party dependency, consider using the
   `~pyrseus.ctx.plugins.loky` plugin instead of this one. Here are a few of its
   advantages:

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


import importlib
from functools import cached_property

from pyrseus.ctx.plugins.process import EntryPoint as _BaseEntryPoint


class EntryPoint(_BaseEntryPoint):
    @cached_property
    def is_available(self):
        return all(
            importlib.util.find_spec(mod_name) is not None
            for mod_name in ("cloudpickle", "pyrseus.executors.cpprocess")
        )

    @cached_property
    def ExecutorCls(self):
        from pyrseus.executors.cpprocess import CpProcessPoolExecutor

        return CpProcessPoolExecutor


ENTRY_POINT = EntryPoint()
